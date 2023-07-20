docker build -t gwtm_cron .
docker tag gwtm_cron:latest 929887798640.dkr.ecr.us-east-2.amazonaws.com/gwtm_cron_listener:latest
./ecrlogin.sh
docker push 929887798640.dkr.ecr.us-east-2.amazonaws.com/gwtm_cron_listener:latest
