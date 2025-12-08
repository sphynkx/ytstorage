This is storage service for [YurTube app](https://github.com/sphynkx/yurtube). Based on gRPC/protobuf. Service is in MVP stage. Only local FS support partly implemented.


## Install and config
```bash
cd /opt
git clone https://github.com/sphynkx/ytstorage
cd ytstorage
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r install/requirements.txt
deactivate
chmod a+x run.sh
```
Optionally set necessary params (see `config/config.py`). Make sure that `proto/ytstorage.proto` is same as on at `yurtube` installation. Otherwise just run `gen_proto.sh` in same dir.

Finally run:
```bash
./run.sh
```
