import re
import sys

pattern = re.compile(r"unistr\('((?:''|[^'])*)'\)")

for line in sys.stdin:
    sys.stdout.write(pattern.sub(lambda m: "'" + m.group(1) + "'", line))
