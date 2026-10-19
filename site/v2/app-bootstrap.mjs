import {startWorkspace} from "./app.mjs?v=1cb143ddb56e";
import {deploymentFor} from "./deployment.mjs?v=1cb143ddb56e";
startWorkspace(deploymentFor(location));
