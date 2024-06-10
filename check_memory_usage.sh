#/bin/bash
# Script to check the memory use of the container. Run alongside the container.

echo $1
check_memory_usage() {
  maxTime=${1:-300}
  start=$(date +%s)
  hasStarted=0
  while true; do
    if [ $(date +%s) -gt $(expr ${start} + ${maxTime}) ]; then
      break
    fi
    stats=$(docker stats --format '{{.Name}}\t{{.MemPerc}}\t{{.MemUsage}}' --no-stream)

    if [ -z "${stats}" ]; then
      if [ ${hasStarted} -eq 1 ]; then
        break
      fi
      continue
    fi
    hasStarted=1
    echo "$(date "+%Y-%m-%d %H:%M:%S")\t${stats}"
  done
}

check_memory_usage $@
