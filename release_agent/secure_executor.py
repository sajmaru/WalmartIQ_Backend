# release_agent/secure_executor.py

import asyncio
import json
import os
import tempfile
import subprocess
import sys
import time
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class SecureCodeExecutor:
    async def _execute_in_docker(self, code: str, working_directory: str = None) -> Dict[str, Any]:
        """Execute code in a Docker container for maximum security."""
        
        # Create a secure execution script
        secure_script = self._create_secure_script(code, working_directory)
        
        # Create temporary directory for Docker execution
        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = os.path.join(temp_dir, 'execute.py')
            
            with open(script_path, 'w') as f:
                f.write(secure_script)
            
            # Copy KG files to temp directory if working_directory is provided
            if working_directory and os.path.exists(working_directory):
                kg_temp_dir = os.path.join(temp_dir, 'KGs')
                os.makedirs(kg_temp_dir, exist_ok=True)
                
                for file in os.listdir(working_directory):
                    if file.endswith('.json'):
                        src = os.path.join(working_directory, file)
                        dst = os.path.join(kg_temp_dir, file)
                        with open(src, 'r') as src_f, open(dst, 'w') as dst_f:
                            dst_f.write(src_f.read())
            
            # Docker run command
            docker_cmd = [
                'docker', 'run', '--rm',
                '--memory', f'{self.max_memory_mb}m',
                '--cpus', '0.5',
                '--network', 'none',  # No network access
                '--read-only',  # Read-only filesystem
                '--tmpfs', '/tmp',  # Writable tmp
                '-v', f'{temp_dir}:/workspace:ro',  # Mount workspace as read-only
                'python:3.11-slim',
                'python', '/workspace/execute.py'
            ]
            
            try:
                process = await asyncio.create_subprocess_exec(
                    *docker_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.execution_timeout
                )
                
                if process.returncode == 0:
                    try:
                        # Parse JSON results from stdout
                        output_lines = stdout.decode().strip().split('\n')
                        for line in reversed(output_lines):
                            if line.strip():
                                try:
                                    result_data = json.loads(line)
                                    return {
                                        'success': True,
                                        'result': result_data,
                                        'stdout': stdout.decode(),
                                        'stderr': stderr.decode()
                                    }
                                except json.JSONDecodeError:
                                    continue
                        
                        return {
                            'success': True,
                            'result': {'data': [], 'raw_output': stdout.decode()},
                            'stdout': stdout.decode(),
                            'stderr': stderr.decode()
                        }
                        
                    except Exception as e:
                        return {
                            'success': False,
                            'error': f'Failed to parse Docker results: {str(e)}',
                            'stdout': stdout.decode(),
                            'stderr': stderr.decode()
                        }
                else:
                    return {
                        'success': False,
                        'error': f'Docker execution failed with return code {process.returncode}',
                        'stdout': stdout.decode(),
                        'stderr': stderr.decode()
                    }
                    
            except Exception as e:
                logger.error(f"Docker execution error: {e}")
                return {
                    'success': False,
                    'error': f'Docker execution error: {str(e)}'
                }
    
    def _create_secure_script(self, code: str, working_directory: str = None) -> str:
        """Create a secure Python script wrapper for the generated code."""
        
        # Adjust working directory path for the execution environment
        kg_path = 'KGs' if working_directory else 'Data/KGs'
        
        secure_wrapper = f'''
import sys
import json
import os
import signal
import resource
from datetime import datetime
import traceback

# Set resource limits
try:
    # Limit memory usage to {self.max_memory_mb}MB
    resource.setrlimit(resource.RLIMIT_AS, ({self.max_memory_mb * 1024 * 1024}, {self.max_memory_mb * 1024 * 1024}))
    
    # Limit CPU time to {self.execution_timeout} seconds
    resource.setrlimit(resource.RLIMIT_CPU, ({self.execution_timeout}, {self.execution_timeout}))
except:
    pass  # Resource limits might not be available on all systems

# Timeout handler
def timeout_handler(signum, frame):
    raise TimeoutError("Code execution timed out")

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm({self.execution_timeout})

# Restricted imports - only allow safe modules
allowed_modules = {self.allowed_imports}

class RestrictedImporter:
    def __init__(self, allowed_modules):
        self.allowed_modules = allowed_modules
    
    def find_spec(self, name, path, target=None):
        if name.split('.')[0] not in self.allowed_modules:
            raise ImportError(f"Import of '{{name}}' is not allowed")
        return None

# Install import hook
sys.meta_path.insert(0, RestrictedImporter(allowed_modules))

# Safe execution environment
safe_globals = {{
    '__builtins__': {{
        'len': len, 'range': range, 'enumerate': enumerate,
        'zip': zip, 'map': map, 'filter': filter, 'sorted': sorted,
        'sum': sum, 'min': max, 'max': max, 'abs': abs,
        'round': round, 'bool': bool, 'int': int, 'float': float,
        'str': str, 'list': list, 'dict': dict, 'set': set,
        'tuple': tuple, 'type': type, 'isinstance': isinstance,
        'hasattr': hasattr, 'getattr': getattr, 'setattr': setattr,
        'print': print, 'Exception': Exception, 'ValueError': ValueError,
        'TypeError': TypeError, 'KeyError': KeyError, 'IndexError': IndexError
    }}
}}

try:
    # Pre-import allowed modules to avoid import restrictions
    import json
    import networkx as nx
    import pandas as pd
    import numpy as np
    from datetime import datetime
    from collections import defaultdict, Counter
    import itertools
    import math
    
    # Add to safe globals
    safe_globals.update({{
        'json': json, 'nx': nx, 'pd': pd, 'np': np,
        'datetime': datetime, 'defaultdict': defaultdict,
        'Counter': Counter, 'itertools': itertools, 'math': math
    }})
    
    # Change to appropriate working directory
    if os.path.exists('{kg_path}'):
        os.chdir(os.path.dirname('{kg_path}'))
    
    # Execute user code
    exec("""
{code}
""", safe_globals)
    
    # Output results as JSON
    if 'results' in safe_globals:
        print(json.dumps(safe_globals['results']))
    else:
        print(json.dumps({{"error": "No 'results' variable found in code"}}))

except Exception as e:
    error_result = {{
        "error": str(e),
        "traceback": traceback.format_exc(),
        "data": [],
        "metadata": {{"execution_failed": True}}
    }}
    print(json.dumps(error_result))

finally:
    signal.alarm(0)  # Cancel the alarm
'''
        return secure_wrapper
    
    def validate_code_safety(self, code: str) -> tuple[bool, str]:
        """
        Validate that the generated code is safe to execute.
        
        Returns:
            (is_safe, reason)
        """
        dangerous_patterns = [
            # File system operations
            r'open\s*\(',
            r'file\s*\(',
            r'os\.',
            r'sys\.',
            r'subprocess',
            r'eval\s*\(',
            r'exec\s*\(',
            r'compile\s*\(',
            r'__import__',
            
            # Network operations
            r'urllib',
            r'requests',
            r'socket',
            r'http',
            
            # System operations
            r'os\.system',
            r'os\.popen',
            r'os\.spawn',
            r'subprocess',
            
            # Dangerous builtins
            r'globals\s*\(\)',
            r'locals\s*\(\)',
            r'vars\s*\(\)',
            r'dir\s*\(\)',
            r'getattr',
            r'setattr',
            r'delattr',
            r'hasattr',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                return False, f"Potentially dangerous pattern detected: {pattern}"
        
        # Check for allowed imports only
        import_pattern = r'(?:from\s+(\w+)|import\s+(\w+))'
        imports = re.findall(import_pattern, code)
        
        for from_import, direct_import in imports:
            module = from_import or direct_import
            if module and module not in self.allowed_imports:
                return False, f"Import of '{module}' is not allowed"
        
        return True, "Code appears safe"

    async def test_execution(self) -> Dict[str, Any]:
        """Test the execution environment with a simple query."""
        test_code = '''
import json
import networkx as nx

# Simple test
results = {
    "test": "success",
    "data": [{"node": "test", "value": 42}],
    "metadata": {"test_execution": True},
    "summary": {"total_records": 1}
}
'''
        
        logger.info("Testing secure execution environment...")
        result = await self.execute(test_code)
        
        if result.get('success'):
            logger.info("✅ Secure execution test passed")
        else:
            logger.error(f"❌ Secure execution test failed: {result.get('error')}")
        
        return result
    
    def __init__(self, 
                 execution_timeout: int = 30,
                 max_memory_mb: int = 512,
                 allowed_imports: list = None):
        self.execution_timeout = execution_timeout
        self.max_memory_mb = max_memory_mb
        self.allowed_imports = allowed_imports or [
            'json', 'networkx', 'pandas', 'numpy', 'datetime', 'collections', 'itertools', 'math'
        ]
        self.execution_strategy = self._determine_execution_strategy()
    
    def _determine_execution_strategy(self) -> str:
        """Determine the best available execution strategy."""
        # Check if Docker is available
        try:
            subprocess.run(['docker', '--version'], capture_output=True, check=True)
            return 'docker'
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        # Check if we can use subprocess safely
        return 'subprocess'
    
    async def execute(self, code: str, working_directory: str = None) -> Dict[str, Any]:
        """
        Execute code safely using the best available strategy.
        
        Args:
            code: Python code to execute
            working_directory: Directory containing KG files
            
        Returns:
            Dictionary with execution results
        """
        start_time = time.time()
        
        try:
            if self.execution_strategy == 'docker':
                result = await self._execute_in_docker(code, working_directory)
            else:
                result = await self._execute_in_subprocess(code, working_directory)
            
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            
            return result
            
        except asyncio.TimeoutError:
            return {
                'success': False,
                'error': f'Code execution timed out after {self.execution_timeout} seconds',
                'execution_time': time.time() - start_time
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Execution failed: {str(e)}',
                'execution_time': time.time() - start_time
            }
    
    async def _execute_in_subprocess(self, code: str, working_directory: str = None) -> Dict[str, Any]:
        """Execute code in a subprocess with restrictions."""
        
        # Create a secure execution script
        secure_script = self._create_secure_script(code, working_directory)
        
        # Write script to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(secure_script)
            script_path = f.name
        
        try:
            # Execute with timeout and capture output
            process = await asyncio.create_subprocess_exec(
                sys.executable, script_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=working_directory
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.execution_timeout
            )
            
            # Parse results
            if process.returncode == 0:
                try:
                    # Try to parse the last line as JSON (our results)
                    output_lines = stdout.decode().strip().split('\n')
                    for line in reversed(output_lines):
                        if line.strip():
                            try:
                                result_data = json.loads(line)
                                return {
                                    'success': True,
                                    'result': result_data,
                                    'stdout': stdout.decode(),
                                    'stderr': stderr.decode()
                                }
                            except json.JSONDecodeError:
                                continue
                    
                    # If no JSON found, return raw output
                    return {
                        'success': True,
                        'result': {'data': [], 'raw_output': stdout.decode()},
                        'stdout': stdout.decode(),
                        'stderr': stderr.decode()
                    }
                    
                except Exception as e:
                    return {
                        'success': False,
                        'error': f'Failed to parse results: {str(e)}',
                        'stdout': stdout.decode(),
                        'stderr': stderr.decode()
                    }
            else:
                return {
                    'success': False,
                    'error': f'Script failed with return code {process.returncode}',
                    'stdout': stdout.decode(),
                    'stderr': stderr.decode()
                }
                
        finally:
            # Clean up temporary file
            try:
                os.unlink(script_path)
            except:
                pass