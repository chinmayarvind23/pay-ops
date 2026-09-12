# Optional AWS setup

## Host the replay yourself

1. Install Docker, AWS CLI v2 and the Lightsail control plugin. Sign in to your own
   AWS account and choose a region. Check `aws sts get-caller-identity` and current
   Lightsail pricing before creating resources.
2. Follow `infra/replay/README.md` to build the isolated replay directory and
   `payops-replay:release` Linux image. Verify its local `/health` endpoint.
3. In the Lightsail console, create a container service with scale 1 and a capacity
   appropriate for a small static Nginx server. Record the service name and region.
4. Push the image using your own service name and region:

   ```bash
   aws lightsail push-container-image --region YOUR_REGION --service-name YOUR_SERVICE --label replay --image payops-replay:release
   ```

5. Create a deployment in that service. Select the exact image identifier returned
   by the push, expose container port `7860` as HTTP, select that container and port
   as the public endpoint, and set the health-check path to `/health`. Add no secrets.
6. Wait for the deployment to become active. Open its generated HTTPS URL, check
   `/health`, select all four incidents and inspect their citations. Keep the source
   revision, image digest and deployment version with your release notes.
7. To undo a release, deploy the prior known image version. To stop charges, delete
   the service after saving evidence; disabling it does not stop service charges.

References: [Lightsail container services](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-container-services.html),
[create a service](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-creating-container-services.html).
