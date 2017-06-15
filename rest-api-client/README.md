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

## Example of assigning participants to a test HPO:

```
./run_client.sh --project <PROJECT> --account <ACCOUNT> hpo_assigner.py --file participant_ids.csv [--hpo <HPO>]
```

where participant_ids.csv is a file containing a list of participant IDs without the leading 'P', e.g.:

```
123456789
234567890
```

and <HPO> is the name of a HPO, e.g. PITT; defaults to TEST.

