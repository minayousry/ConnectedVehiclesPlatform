# install vsc for redhat
sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
sudo sh -c 'echo -e "[code]\nname=Visual Studio Code\nbaseurl=https://packages.microsoft.com/yumrepos/vscode\nenabled=1\ngpgcheck=1\ngpgkey=https://packages.microsoft.com/keys/microsoft.asc" > /etc/yum.repos.d/vscode.repo'

dnf check-update
sudo dnf install code # or code-insiders

install kafka
sudo dnf install python3-pip


# Configure server properties
## 1-get ip address:
ip addr show

## 2-get the ip address beside inet

## 3-replace the ip address in the server.properties in the following lines
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://your.vm.ip.address:9092

## 4-sudo dnf install dbus-x11
eval $(dbus-launch --sh-syntax)

## 5-open ports in the firewall of RHEL 9:
sudo firewall-cmd --zone=public --add-port=5432/tcp --permanent
sudo firewall-cmd --reload
sudo setenforce 0


## 6-Run Kafka using 2 commands
zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties
kafka-server-start.sh $HOME/kafka/config/server.properties


# =======================GreenPlub================
## 1-mkdir gpconfigs
## 2-cp /usr/local/greenplum-db-7.1.0/docs/cli_help/gpconfigs/gpinitsystem_config ~/gpconfigs/my_gpinitsystem_config
## 3-modify the following in my_gpinitsystem_config
COORDINATOR_HOSTNAME=localhost
MACHINE_LIST_FILE=/home/msamy/gpconfigs/hostfile_gpinitsystem

## 4-touch ~/gpconfigs/hostfile_gpinitsystem

## 5-in the file add:
host1.example.com
host2.example.com


## 6-Run the following command:
echo "localhost" > ~/gpconfigs/hostfile.txt
sudo mkdir -p /data/coordinator
sudo chown msamy:msamy /data/coordinator
chmod 700 /data/coordinator
ssh-keygen -t rsa -b 2048
chmod 700 ~/.ssh
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
source /usr/local/greenplum-db/greenplum_path.sh
sudo mkdir -p /data1/primary
sudo chown msamy:msamy /data1/primary
chmod 700 /data1/primary

sudo mkdir -p /data2/primary
sudo chown msamy:msamy /data2/primary
chmod 700 /data2/primary

## 7-Create and intialize database:
gpinitsystem -c ~/gpconfigs/my_gpinitsystem_config -h ~/gpconfigs/hostfile.txt

## 8-Set the Environment configuration for the shell
source /usr/local/greenplum-db-7.1.0/greenplum_path.sh
export COORDINATOR_DATA_DIRECTORY=/data/coordinator/gpseg-1

## 9-Verify the Greenplum Installation:
gpstate -d /data/coordinator/gpseg-1


# in Python
pip install psycopg2-binary


# To run greenPlum database
source /usr/local/greenplum-db-7.1.0/greenplum_path.sh
export COORDINATOR_DATA_DIRECTORY=/data/coordinator/gpseg-1
gpstart
