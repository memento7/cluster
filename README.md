# memento cluster
memento cluster

# Insatllation
```
apt-get update
apt-get install python3 python3-pip git -y
apt-get install software-properties-common -y

// for jdk installation
add-apt-repository ppa:openjdk-r/ppa -y
apt-get update
apt-get install openjdk-8-jdk -y

git clone https://github.com/memento7/cluster.git
cd cluster
pip3 install -r requirements.txt

export MEMENTO_ELASTIC=''
export MEMENTO_ELASTIC_PASS=''
```

# Usage
```
python3 -c "from app import process;
process(entity, date_start, date_end, 'id')"
```

date format: "yyyy.mm.dd"
