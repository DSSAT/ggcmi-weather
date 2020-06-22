#!/usr/bin/env bash

#SBATCH --job-name=ggcmi_test
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4GB
#SBATCH --nodes=17
#SBATCH --tasks-per-node 1
#SBATCH --time=5-53:00:00

worker_num=16 # Must be one less that the total number of nodes

module load python-core # This will vary depending on your environment
source /ufrc/hoogenboom/share/hg_config/venvs/ggcmi-ray-remote/bin/activate

nodes=$(scontrol show hostnames $SLURM_JOB_NODELIST) # Getting the node names
nodes_array=( $nodes )

node1=${nodes_array[0]}

ip_prefix=$(srun --nodes=1 --ntasks=1 -w $node1 hostname --ip-address) # Making address
suffix=':6379'
ip_head=$ip_prefix$suffix
redis_password=$(uuidgen)

export ip_head # Exporting for latter access by trainer.py

srun --nodes=1 --ntasks=1 -w $node1 ray start --block --head --redis-port=6379 --redis-password=$redis_password & # Starting the head
sleep 5
# Make sure the head successfully starts before any worker does, otherwise
# the worker will not be able to connect to redis. In case of longer delay,
# adjust the sleeptime above to ensure proper order.

for ((  i=1; i<=$worker_num; i++ ))
do
  node2=${nodes_array[$i]}
  srun --nodes=1 --ntasks=1 -w $node2 ray start --block --address=$ip_head --redis-password=$redis_password & # Starting the workers
  # Flag --block will keep ray process alive on each compute node.
  sleep 5
done

srun python -u /ufrc/hoogenboom/share/apps/ggcmi-weather/ggcmi-weather.py $redis_password 17