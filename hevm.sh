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
    *)
      echo $1 not found
      ;;
  esac
