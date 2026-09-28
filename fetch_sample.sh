#!/bin/zsh
# One-off: fetch the Yocale PCJL appointments CSV so we can see its columns.
# Tries the plausible readings of the username Yocale sent ("U:PCJLjl-pcjl-reports").
# The password is read silently, never echoed, never written to disk or history.
# Usage: zsh ~/Developer/yocale/fetch_sample.sh
set -u
out="${0:A:h}/pcjl_sample.csv"
url="https://jiffylube.yocale.com/reports/pcjl/appointments-daily.csv"
users=(PCJLjl-pcjl-reports jl-pcjl-reports pcjl-reports PCJL)

printf 'Yocale CSV password (typing is hidden): '
if ! read -rs YPASS; then echo; echo "No input read - aborted."; exit 1; fi
echo
if [[ -z "$YPASS" ]]; then echo "Empty password - aborted."; exit 1; fi

for u in $users; do
  code=$(curl -sS -u "$u:$YPASS" -o "$out.try" -w '%{http_code}' "$url")
  printf '%-24s HTTP %s\n' "$u" "$code"
  if [[ "$code" == 200 ]]; then
    mv "$out.try" "$out"
    unset YPASS
    echo "Worked as '$u'. Saved $(wc -l < "$out" | tr -d ' ') lines to $out"
    exit 0
  fi
done

unset YPASS
rm -f "$out.try"
echo "No username worked with that password."
