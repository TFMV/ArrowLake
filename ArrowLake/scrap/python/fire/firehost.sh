# Set up Firebase Hosting in front of a Cloud Run service, without using the firebase CLI
# The following commands must be installed:
# - gcloud
# - curl
# - jq

# Update these variables
PROJECT_ID="enable-fb-hosting" # Make sure you have enabled Firebase on this Google Cloud project
CLOUD_RUN_SERVICE_NAME="hello"
CLOUD_RUN_SERVICE_REGION="us-central1"


ACCESS_TOKEN=$(gcloud auth print-access-token) #Make sure you are logged into gcloud

# Inspired by https://firebase.google.com/docs/hosting/api-deploy

echo "Creating new Firebase Hosting version:"

version=$(
    curl --silent -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -d '{
            "config": {
                "rewrites": [{
                    "glob": "**",
                    "run": {
                        "serviceId": "'$CLOUD_RUN_SERVICE_NAME'",
                        "region": "'$CLOUD_RUN_SERVICE_REGION'"
                    }
                }]
            }
        }' \
https://firebasehosting.googleapis.com/v1beta1/sites/$PROJECT_ID/versions \
| jq -r '.name')

echo "New version created: $version"

echo "Finalizing..."

curl --silent -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -X PATCH \
        -d '{"status": "FINALIZED"}' \
https://firebasehosting.googleapis.com/v1beta1/$version?update_mask=status \
| jq '.status'

echo "Releasing..."

curl --silent -H "Authorization: Bearer $ACCESS_TOKEN" \
      -X POST https://firebasehosting.googleapis.com/v1beta1/sites/$PROJECT_ID/releases?versionName=$version \
| jq '.version.status'

echo "Cloud Run service $CLOUD_RUN_SERVICE_NAME ($CLOUD_RUN_SERVICE_REGION) is serving behind Firebase Hosting at https://$PROJECT_ID.web.app/"

echo "Set up a custom domain at https://console.firebase.google.com/project/$PROJECT_ID/hosting/sites"
