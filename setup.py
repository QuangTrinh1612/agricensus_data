import sys
import subprocess

# implement pip as a subprocess:
# subprocess.check_call([sys.executable, '-m', 'pipenv', 'uninstall', 'bs4'])
subprocess.check_call([sys.executable, '-m', 'pipenv', 'install', 'google-api-python-client', 'oauth2client', 'requests'])
# subprocess.check_call([sys.executable, '-m', 'pipenv', 'graph'])

# process output with an API in the subprocess module:
# reqs = subprocess.check_output([sys.executable, '-m', 'pipenv', 'freeze'])
# installed_packages = [r.decode().split('==')[0] for r in reqs.split()]

# print(installed_packages)