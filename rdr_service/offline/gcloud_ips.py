# To support validating AppEngine calls by IP whitelisting, we need to know
# which DNS blocks are used by AppEngine.  This code implements the algorithm
# described at https://cloud.google.com/appengine/kb/ for recursively expanding
# DNS blocks used by AppEngine.

# Three example values of the TXT record returned from these requests:

# pylint: disable=line-too-long
"""
_cloud-netblocks.googleusercontent.com. 3599 IN TXT "v=spf1 include:_cloud-netblocks1.googleusercontent.com include:_cloud-netblocks2.googleusercontent.com include:_cloud-netblocks3.googleusercontent.com include:_cloud-netblocks4.googleusercontent.com include:_cloud-netblocks5.googleusercontent.com ?all"
_cloud-netblocks.googleusercontent.com. 3599 IN TXT "v=spf1 include:_cloud-netblocks1.googleusercontent.com include:_cloud-netblocks2.googleusercontent.com include:_cloud-netblocks3.googleusercontent.com include:_cloud-netblocks4.googleusercontent.com include:_cloud-netblocks5.googleusercontent.com ?all"
_cloud-netblocks5.googleusercontent.com. 3599 IN TXT "v=spf1 ip6:2600:1900::/35 ?all"
"""
# pylint: enable=line-too-long

import dns.resolver
import json
import re
from  collections import namedtuple

START = "_cloud-netblocks.googleusercontent.com"
Response = namedtuple("Response", ["next_entries", "ip4", "ip6"])

def get_ip_ranges(start):
  """Entry point for recursive disocvery of IP blocks"""
  resolved_blocks = {}
  explore(Response([start], [], []), resolved_blocks)
  return Response(
    [],
    [v for r in resolved_blocks.values() for v in r.ip4],
    [v for r in resolved_blocks.values() for v in r.ip6])

def lookup_txt(domain):
  """Return the TXT record associated with a domain"""
  return dns.resolver.query(domain, "TXT").rrset.to_text()

def resolve(source):
  """Find all next-hops, ip4 blocks,and ip6 blocks for a domain"""
  txt = lookup_txt(source)
  return Response(
        list(re.findall(r"include:(.*?\.googleusercontent\.com)", txt)),
        list(re.findall(r"ip4:([0-9\.\/:]+)", txt)),
        list(re.findall(r"ip6:([0-9\.\/:]+)", txt))
  )

def explore(to_visit, resolved_blocks):
  """Depth-first search to find ip blocks"""
  for next_entry in to_visit.next_entries:
    if next_entry not in resolved_blocks:
      response = resolve(next_entry)
      resolved_blocks[next_entry] = response
      explore(response, resolved_blocks)

def print_json_for_debug():
  """Heler to print the entire AppEngine IP range to stdout, in JSON"""
  ips = get_ip_ranges(START)
  print json.dumps({'ip4': ips.ip4, 'ip6': ips.ip6}, indent=2)


if __name__ == "__main__":
  print_json_for_debug()
