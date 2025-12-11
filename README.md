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
Optionally set necessary params (see `config/config.py`). Make sure that `proto/ytstorage.proto` is same as one at `yurtube` installation. Otherwise just run `gen_proto.sh` in same dir.

Finally run:
```bash
./run.sh
```


## Run as systemd service
```bash
cp install/ytstorage.service /etc/systemd/system
systemctl daemon-reload
systemctl enable ytstorage
systemctl start ytstorage
systemctl status ytstorage
```


## Test
You may use special util for quick health test:
```bash
dnf install grpcurl
```
and:
```bash
grpcurl -plaintext 127.0.0.1:50070 list
grpcurl -plaintext 127.0.0.1:50070 describe storage.StorageService
```


## S3 configuration
Service may use S3 based storages via `aiobotocore` module (support for S3, Ceph, Google Cloud, MinIO etc). Ir requires configured and available local or remote storage. Below is a quick simple configuration for local minio based storage.


### MinIO
First, download  MinIO binary and install systemd unit file (Make sure credentials in `ytminio.service` file match your desired config!!) - either on the same server or a remote one:
```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio -O /usr/local/bin/minio
chmod a+x /usr/local/bin/minio
mkdir -p /var/lib/minio_data
cd opt/ytstorage
cp install/ytminio.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now ytminio
```


### YTStorage service configuration
Add to `.env`, modify your creds:
```cfg
DRIVER_KIND=s3
S3_ENDPOINT_URL=http://127.0.0.1:9000
S3_ACCESS_KEY_ID=admin
S3_SECRET_ACCESS_KEY=password123
S3_BUCKET_NAME=yurtube-bucket
```


### Rclone setup
This is util for comfortable manipulation of S3 storage and migration. Install it and run config:
```bash
dnf install rclone
rclone config
```
This is command line configuration. You need choose:
* n (New remote)
* name: __yt_minio__
* Storage: __s3__ (mostly it will be at p.5)
* provider: __Minio__ (mostly it will be at p.17)
env_auth: __false__
access_key_id: __admin__
secret_access_key: __password123__ (set same as in ytminio.service and ytstorage config!!)
region: __us-east-1__ (set it manually, dont sure on defaults!!).
endpoint: __http://127.0.0.1:9000__

Rest params - leave empty, just press Enter. "Advanced config" - no. Finally press `q` to quit. Configuration stored in `~/.config/rclone/rclone.conf`.

Check:
```bash
rclone lsd yt_minio:
```
Empty output is OK (no buckets yet). Error "SignatureDoesNotMatch" means wrong password or region. If you previously run `ytminio` service - you will see "yurtube-bucket". Also you may create bucket manually:
```bash
rclone mkdir yt_minio:yurtube-bucket
```
For manual manipulations you may create mount dir and mount storage:
```bash
rclone mkdir yt_minio:yurtube-bucket
mkdir -p /mnt/minio_mount
rclone mount yt_minio:yurtube-bucket /mnt/minio_mount --daemon
```

Troubleshootings:
* "SignatureDoesNotMatch": This usually means the credentials entered in rclone config do not match the MinIO server credentials. Also, double-check that region is set to __us-east-1__.
* "Connection refused": Check if MinIO service is running (systemctl status ytminio).


### Migration from local storage to S3
Set ytstorage service to s3 driver and stop it before migration. Run migration, finally start service again.
```bash
service ytstorage stop
rclone copy /var/www/yurtube/storage yt_minio:yurtube-bucket --progress
service ytstorage start
```
Check is app shows all content.
