case $1 in
    ssh)
      gcloud compute ssh $HELIUM_VM --zone=$HELIUM_ZONE
      ;;
    list)
      gcloud compute instances list
      ;;
    start)
      gcloud compute instances start $HELIUM_VM --zone=$HELIUM_ZONE
      ;;
    stop)
      gcloud compute instances stop $HELIUM_VM --zone=$HELIUM_ZONE
      ;;
    normal)
      gcloud compute instances set-machine-type $HELIUM_VM --zone=$HELIUM_ZONE --machine-type e2-medium
      ;;
    turbo)
      gcloud compute instances set-machine-type $HELIUM_VM --zone=$HELIUM_ZONE --machine-type e2-highmem-16
      ;;
    newip)
      gcloud compute instances delete-access-config $HELIUM_VM  --zone=$HELIUM_ZONE --access-config-name="External NAT"
      gcloud compute addresses delete $HELIUM_VM-ip --region=$HELIUM_REGION --quiet
      gcloud compute addresses create $HELIUM_VM-ip --region=$HELIUM_REGION
      HELIUM_IP=`gcloud compute addresses describe $HELIUM_VM-ip --region=$HELIUM_REGION | grep 'address:' | cut  -d ' ' -f 2`
      gcloud compute instances add-access-config $HELIUM_VM --zone=$HELIUM_ZONE --access-config-name="External NAT" --address=$HELIUM_IP
      sudo sed -i '' -e "s/.*$HELIUM_VM/$HELIUM_IP	$HELIUM_VM/g" /etc/hosts
      echo "Hostname $HELIUM_IP" >$HOME/.ssh/config.$HELIUM_VM
      echo "New external IP address: $HELIUM_IP"
      ;;
    *)
      echo $1 not found
      ;;
  esac
