import {startWorkspace} from "./app.mjs?v=34e7f57be8d1";
import {deploymentFor} from "./deployment.mjs?v=34e7f57be8d1";
startWorkspace(deploymentFor(location));
