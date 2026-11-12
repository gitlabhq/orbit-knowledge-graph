#!/bin/sh
# Runs on every launch, since a VM restart drops these rules.
set -eu
edge=$1
internal=$2

systemctl mask --now apt-daily.timer apt-daily-upgrade.timer motd-news.timer ua-timer.timer \
  systemd-timesyncd.service >/dev/null 2>&1

for tool in iptables ip6tables; do
  $tool -N ORBIT-DEMO-OUT 2>/dev/null || $tool -F ORBIT-DEMO-OUT
  $tool -A ORBIT-DEMO-OUT -o lo -j RETURN
  $tool -A ORBIT-DEMO-OUT -m conntrack --ctstate ESTABLISHED,RELATED -j RETURN
  if [ "$tool" = iptables ]; then
    $tool -A ORBIT-DEMO-OUT -d 172.16.0.0/12 -j RETURN
  fi
  $tool -A ORBIT-DEMO-OUT -j DROP
  $tool -C OUTPUT -j ORBIT-DEMO-OUT 2>/dev/null || $tool -I OUTPUT 1 -j ORBIT-DEMO-OUT
done

for bridge in "$edge" "$internal"; do
  rule="-i $bridge ! -o $bridge -m conntrack --ctstate NEW -j DROP"
  iptables -C DOCKER-USER $rule 2>/dev/null || iptables -I DOCKER-USER 1 $rule
done
echo "VM egress locked: OUTPUT -> ORBIT-DEMO-OUT, DOCKER-USER drops new flows out of $edge and $internal"
