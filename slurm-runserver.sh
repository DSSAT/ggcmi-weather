#!/usr/bin/env bash

#SBATCH --job-name=GGCMI-SRV
#SBATCH --mem-per-cpu=4G
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=13
#SBATCH --time=4-12:00:00

module load python-core
source /ufrc/hoogenboom/share/hg_config/venvs/ggcmi-twisted/bin/activate

#Very very parallel
nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
nodes_array=( $nodes )

node1=${nodes_array[0]}

ip_prefix=$(srun --nodes=1 --ntasks=1 -w $node1 hostname --ip-address) # Making address
server_port='7781'

echo "$ip_prefix : $server_port"
srun --nodes=1 --ntasks=1 -w $node1 python -V 
srun --nodes=1 --ntasks=1 -w $node1 python -u gwg-server.py $ip_prefix $server_port
