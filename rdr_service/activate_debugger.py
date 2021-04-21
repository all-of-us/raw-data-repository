import os
if os.getenv('GAE_ENV', '').startswith('standard'):
    try:
        import googleclouddebugger
        googleclouddebugger.enable()
    except ImportError:
        pass
