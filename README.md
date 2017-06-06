# memento cluster
memento cluster

# Insatllation
```
apt-get update
apt-get install python3 python3-pip git -y

git clone https://github.com/mement7/cluster.git
cd cluster
pip3 install -r requirements.txt

export MEMENTO_BASIC=''
export MEMENTO_ELASTIC=''
export MEMENTO_ELASTIC_PASS=''
```

# Usage
```
python -c "
from app import process;
process(entity, date_start, date_end)
"
```

date format: "yyyy.mm.dd"
