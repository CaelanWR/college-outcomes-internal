// Only the existing protected API receives access tokens. No credentials or
// datasets are bundled into the Pages build. Loopback keeps its local relay.
export function deploymentFor({hostname}) {
  if(hostname === "caelanwr.github.io")return {
    apiOrigin:"https://college-outcomes-api.onrender.com",
    pathwaysOrigin:null,
  };
  return {};
}
