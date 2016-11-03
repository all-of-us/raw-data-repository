import dns.resolver
import re
from  collections import namedtuple

Response = namedtuple("Response", ["next_entries", "ip4", "ip6"])

def get_ip_ranges(start):
    resolved_blocks = {}
    explore(Response([start], [], []), resolved_blocks)
    return Response(
        [],
        [v for r in resolved_blocks.values() for v in r.ip4],
        [v for r in resolved_blocks.values() for v in r.ip6])

def resolve(source):
      q = dns.resolver.query(source, "TXT").response.answer[0].to_text()
      return Response(
          list(re.findall("include:(.*?\.googleusercontent\.com)", q)),
          list(re.findall("ip4:([0-9\.\/:]+)", q)),
          list(re.findall("ip6:([0-9\.\/:]+)", q))
      )

def explore(to_visit, resolved_blocks):
    for next_entry in to_visit.next_entries:
        if next_entry not in resolved_blocks:
            response = resolve(next_entry)
            resolved_blocks[next_entry] = response
            explore(response, resolved_blocks)

if __name__ == "__main__":
    import json
    START = "_cloud-netblocks.googleusercontent.com"
    ips = get_ip_ranges(START)
    print(json.dumps({'ip4': ips.ip4, 'ip6': ips.ip6}, indent=2))
