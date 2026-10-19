// Only the protected Render services receive access tokens. No credentials or
// datasets are bundled into the Pages build. Loopback keeps its local relay.
export function deploymentFor({hostname}) {
  if(hostname === "caelanwr.github.io")return {
    apiOrigin:"https://college-outcomes-api.onrender.com",
    pathwaysOrigin:"https://college-outcomes-pathways.onrender.com",
  };
  return {};
}
