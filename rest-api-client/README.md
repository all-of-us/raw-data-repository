# Test Client for Rest API

This is a simple client that tests the rest API. It also serves as an example
for writing a client in python.

See `rest-api/README.md` for instructions on running a local server.

## Set up a virtual Python environment

```sh
sudo pip install virtualenv
# Create a venv/ directory with Python binary, libs, etc.
virtualenv venv
# Update your PATH, PYTHONHOME, and other settings.
source venv/bin/activate
# Install client deps into venv/lib/.
pip install -r requirements.txt
```

## Add fake participants to a local appserver

```sh
python load_fake_participants.py --instance=http://localhost:8080 --count=10
```

Each fake participant is random. Run the script repeatedly to add more.
