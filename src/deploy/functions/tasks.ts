import * as clc from "cli-color";

import Queue from "../../throttler/queue";
import { logger } from "../../logger";
import { RegionalFunctionChanges } from "./deploymentPlanner";
import { OperationResult, OperationPollerOptions, pollOperation } from "../../operation-poller";
import { functionsOrigin, functionsV2Origin } from "../../api";
import { getHumanFriendlyRuntimeName } from "./parseRuntimeAndValidateSDK";
import { deleteTopic } from "../../gcp/pubsub";
import { DeploymentTimer } from "./deploymentTimer";
import { ErrorHandler } from "./errorHandler";
import { FirebaseError } from "../../error";
import * as backend from "./backend";
import * as cloudscheduler from "../../gcp/cloudscheduler";
import * as gcf from "../../gcp/cloudfunctions";
import * as gcfV2 from "../../gcp/cloudfunctionsv2";
import * as cloudrun from "../../gcp/run";
import * as helper from "./functionsDeployHelper";
import * as utils from "../../utils";

interface PollerOptions {
  apiOrigin: string;
  apiVersion: string;
  masterTimeout: number;
}

// TODO: Tune this for better performance.
const gcfV1PollerOptions = {
  apiOrigin: functionsOrigin,
  apiVersion: gcf.API_VERSION,
  masterTimeout: 25 * 60 * 1000, // 25 minutes is the maximum build time for a function
};

const gcfV2PollerOptions = {
  apiOrigin: functionsV2Origin,
  apiVersion: gcfV2.API_VERSION,
  masterTimeout: 25 * 60 * 1000, // 25 minutes is the maximum build time for a function
};

const pollerOptionsByVersion = {
  1: gcfV1PollerOptions,
  2: gcfV2PollerOptions,
};

export type OperationType =
  | "create"
  | "update"
  | "delete"
  | "upsert schedule"
  | "delete schedule"
  | "delete topic"
  | "make public";

export interface DeploymentTask {
  run: () => Promise<void>;
  fn: backend.TargetIds;
  operationType: OperationType;
}

export interface TaskParams {
  projectId: string;
  runtime?: backend.Runtime;
  sourceUrl?: string;
  storageSource?: gcfV2.StorageSource;
  errorHandler: ErrorHandler;
}

/**
 * Cloud Functions Deployments Tasks and Handler
 */

export function createFunctionTask(
  params: TaskParams,
  fn: backend.FunctionSpec,
  sourceToken?: string,
  onPoll?: (op: OperationResult<backend.FunctionSpec>) => void
): DeploymentTask {
  const fnName = backend.functionName(fn);
  const run = async () => {
    utils.logBullet(
      clc.bold.cyan("functions: ") +
        "creating " +
        getHumanFriendlyRuntimeName(params.runtime!) +
        " function " +
        clc.bold(helper.getFunctionLabel(fn)) +
        "..."
    );
    let op: { name: string };
    if (fn.apiVersion === 1) {
      const apiFunction = backend.toGCFv1Function(fn, params.sourceUrl!);
      if (sourceToken) {
        apiFunction.sourceToken = sourceToken;
      }
      op = await gcf.createFunction(apiFunction);
    } else {
      const apiFunction = backend.toGCFv2Function(fn, params.storageSource!);
      op = await gcfV2.createFunction(apiFunction);
    }
    const cloudFunction = await pollOperation<unknown>({
      ...pollerOptionsByVersion[fn.apiVersion],
      pollerName: `create-${fnName}`,
      operationResourceName: op.name,
      onPoll,
    });
    if (!backend.isEventTrigger(fn.trigger)) {
      try {
        if (fn.apiVersion == 1) {
          await gcf.setIamPolicy({
            name: fnName,
            policy: gcf.DEFAULT_PUBLIC_POLICY,
          });
        } else {
          const serviceName = (cloudFunction as gcfV2.CloudFunction).serviceConfig.service!;
          cloudrun.setIamPolicy(serviceName, cloudrun.DEFAULT_PUBLIC_POLICY);
        }
      } catch (err) {
        params.errorHandler.record("warning", fnName, "make public", err.message);
      }
    }
  };
  return {
    run,
    fn,
    operationType: "create",
  };
}

export function updateFunctionTask(
  params: TaskParams,
  fn: backend.FunctionSpec,
  sourceToken?: string,
  onPoll?: (op: OperationResult<gcf.CloudFunction>) => void
): DeploymentTask {
  const fnName = backend.functionName(fn);
  const run = async () => {
    utils.logBullet(
      clc.bold.cyan("functions: ") +
        "updating " +
        getHumanFriendlyRuntimeName(params.runtime!) +
        " function " +
        clc.bold(helper.getFunctionLabel(fn)) +
        "..."
    );
    let opName;
    if (fn.apiVersion == 1) {
      const apiFunction = backend.toGCFv1Function(fn, params.sourceUrl!);
      if (sourceToken) {
        apiFunction.sourceToken = sourceToken;
      }
      opName = (await gcf.updateFunction(apiFunction)).name;
    } else {
      const apiFunction = backend.toGCFv2Function(fn, params.storageSource!);
      opName = (await gcfV2.updateFunction(apiFunction)).name;
    }
    const pollerOptions: OperationPollerOptions = {
      ...pollerOptionsByVersion[fn.apiVersion],
      pollerName: `update-${fnName}`,
      operationResourceName: opName,
      onPoll,
    };
    await pollOperation<void>(pollerOptions);
  };
  return {
    run,
    fn,
    operationType: "update",
  };
}

export function deleteFunctionTask(params: TaskParams, fn: backend.FunctionSpec): DeploymentTask {
  const fnName = backend.functionName(fn);
  const run = async () => {
    utils.logBullet(
      clc.bold.cyan("functions: ") +
        "deleting function " +
        clc.bold(helper.getFunctionLabel(fnName)) +
        "..."
    );
    let res: { name: string };
    if (fn.apiVersion == 1) {
      res = await gcf.deleteFunction(fnName);
    } else {
      res = await gcfV2.deleteFunction(fnName);
    }
    const pollerOptions: OperationPollerOptions = {
      ...pollerOptionsByVersion[fn.apiVersion],
      pollerName: `delete-${fnName}`,
      operationResourceName: res.name,
    };
    await pollOperation<void>(pollerOptions);
  };
  return {
    run,
    fn,
    operationType: "delete",
  };
}

export function functionsDeploymentHandler(
  timer: DeploymentTimer,
  errorHandler: ErrorHandler
): (task: DeploymentTask) => Promise<any | undefined> {
  return async (task: DeploymentTask) => {
    let result;
    const fnName = backend.functionName(task.fn);
    try {
      timer.startTimer(fnName, task.operationType);
      result = await task.run();
      helper.printSuccess(task.fn, task.operationType);
    } catch (err) {
      if (err.original?.context?.response?.statusCode === 429) {
        // Throw quota errors so that throttler retries them.
        throw err;
      }
      errorHandler.record("error", fnName, task.operationType, err.original?.message || "");
    }
    timer.endTimer(fnName);
    return result;
  };
}

/**
 * Adds tasks to execute all function creates and updates for a region to the provided queue.
 */
export async function runRegionalFunctionDeployment(
  params: TaskParams,
  region: string,
  regionalDeployment: RegionalFunctionChanges,
  queue: Queue<DeploymentTask, void>
): Promise<void> {
  // Build an onPoll function to check for sourceToken and queue up the rest of the deployment.
  const onPollFn = (op: any) => {
    // We should run the rest of the regional deployment if we either:
    // - Have a sourceToken to use.
    // - Never got a sourceToken back from the operation. In this case, finish the deployment without using sourceToken.
    const shouldFinishDeployment =
      (op.metadata?.sourceToken && !regionalDeployment.sourceToken) ||
      (!op.metadata?.sourceToken && op.done);
    if (shouldFinishDeployment) {
      logger.debug(`Got sourceToken ${op.metadata.sourceToken} for region ${region}`);
      regionalDeployment.sourceToken = op.metadata.sourceToken;
      finishRegionalFunctionDeployment(params, regionalDeployment, queue);
    }
  };

  // Choose a first function to deploy.
  // Source tokens are a GCFv1 thing. We need to make sure the first deployed function is a
  // v1 function or all v1 functions will have to rebuild their image.
  const firstV1Create = regionalDeployment.functionsToCreate.findIndex((fn) => fn.apiVersion === 1);
  if (firstV1Create != -1) {
    const firstFn = regionalDeployment.functionsToCreate.splice(firstV1Create)[0];
    const task = createFunctionTask(params, firstFn, /* sourceToken= */ undefined, onPollFn);
    await queue.run(task);
    return;
  }

  const firstV1Update = regionalDeployment.functionsToUpdate.findIndex((fn) => fn.apiVersion === 1);
  if (firstV1Update != -1) {
    const firstFn = regionalDeployment.functionsToUpdate.splice(firstV1Update)[0];
    const task = updateFunctionTask(params, firstFn!, /* sourceToken= */ undefined, onPollFn);
    await queue.run(task);
    return;
  }

  // If all functions are GCFv2 functions, then we just deploy them all in parallel. They'll get
  // throttled when necessary.
  finishRegionalFunctionDeployment(params, regionalDeployment, queue);
}

function finishRegionalFunctionDeployment(
  params: TaskParams,
  regionalChanges: RegionalFunctionChanges,
  queue: Queue<DeploymentTask, void>
): void {
  for (const fn of regionalChanges.functionsToCreate) {
    void queue.run(createFunctionTask(params, fn, regionalChanges.sourceToken));
  }
  for (const fn of regionalChanges.functionsToUpdate) {
    void queue.run(updateFunctionTask(params, fn, regionalChanges.sourceToken));
  }
}

/**
 * Cloud Scheduler Deployments Tasks and Handler
 */

export function upsertScheduleTask(
  params: TaskParams,
  schedule: backend.ScheduleSpec,
  appEngineLocation: string
): DeploymentTask {
  const run = async () => {
    const job = backend.toJob(schedule, appEngineLocation);
    await cloudscheduler.createOrReplaceJob(job);
  };
  return {
    run,
    fn: schedule.targetService,
    operationType: "upsert schedule",
  };
}

export function deleteScheduleTask(
  params: TaskParams,
  schedule: backend.ScheduleSpec,
  appEngineLocation: string
): DeploymentTask {
  const run = async () => {
    const jobName = backend.scheduleName(schedule, appEngineLocation);
    await cloudscheduler.deleteJob(jobName);
  };
  return {
    run,
    fn: schedule.targetService,
    operationType: "delete schedule",
  };
}

export function deleteTopicTask(params: TaskParams, topic: backend.PubSubSpec): DeploymentTask {
  const run = async () => {
    const topicName = backend.topicName(topic);
    await deleteTopic(topicName);
  };
  return {
    run,
    fn: topic.targetService,
    operationType: "delete topic",
  };
}

export const schedulerDeploymentHandler = (errorHandler: ErrorHandler) => async (
  task: DeploymentTask
): Promise<void> => {
  try {
    const result = await task.run();
    helper.printSuccess(task.fn, task.operationType);
    return result;
  } catch (err) {
    if (err.status === 429) {
      // Throw quota errors so that throttler retries them.
      throw err;
    } else if (err.status !== 404) {
      // Ignore 404 errors from scheduler calls since they may be deleted out of band.
      errorHandler.record(
        "error",
        backend.functionName(task.fn),
        task.operationType,
        err.message || ""
      );
    }
  }
};
