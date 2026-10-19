import {startWorkspace} from "./app.mjs?v=20c03c7369ce";
import {deploymentFor} from "./deployment.mjs?v=20c03c7369ce";
startWorkspace(deploymentFor(location));
