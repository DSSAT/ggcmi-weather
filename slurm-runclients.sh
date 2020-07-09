#!/usr/bin/env bash

#SBATCH --job-name=GGCMI-WG
#SBATCH --mem-per-cpu=1G
#SBATCH --nodes=25
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=4-12:00:00

module load python-core
source /ufrc/hoogenboom/share/hg_config/venvs/ggcmi-twisted/bin/activate

#Very very parallel
client_num=25
ip_prefix='172.16.199.25'
server_port='7781'
nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
nodes_array=( $nodes )

for ((  i=0; i<$client_num; i++ ))
do
  node2=${nodes_array[$i]}
  srun --nodes=1 --ntasks=1 -w $node2 python /ufrc/hoogenboom/share/apps/ggcmi-weather/gwg-client.py $ip_prefix $server_port&
  sleep 1
done
wait
