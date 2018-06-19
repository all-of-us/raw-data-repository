from google.cloud import dlp_v2

def deidentify_content(item):
  client = dlp_v2.DlpServiceClient()

  parent = client.project_path('[all-of-rdr-sandbox]')

  response = client.deidentify_content(parent, item=item)

  return response
