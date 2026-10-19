import {startWorkspace} from "./app.mjs";
import {deploymentFor} from "./deployment.mjs";
startWorkspace(deploymentFor(location));
