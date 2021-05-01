import * as clc from "cli-color";
import { setGracefulCleanup } from "tmp";

import { functionsUploadRegion } from "../../api";
import { logSuccess, logWarning } from "../../utils";
import { checkHttpIam } from "./checkIam";
import * as args from "./args";
import * as gcs from "../../gcp/storage";
import * as gcf from "../../gcp/cloudfunctions";
import * as gcfV2 from "../../gcp/cloudfunctionsv2";
import { previews } from "../../previews";
import { logger } from "../../logger";

const GCP_REGION = functionsUploadRegion;

setGracefulCleanup();

async function uploadSourceV1(context: args.Context): Promise<void> {
  const uploadUrl = await gcf.generateUploadUrl(context.projectId, GCP_REGION);
  context.uploadUrl = uploadUrl;
  const apiUploadUrl = uploadUrl.replace("https://storage.googleapis.com", "");
  await gcs.upload(context.functionsSource, apiUploadUrl);
}

async function uploadSourceV2(context: args.Context): Promise<void> {
  if (!previews.functionsv2) {
    return;
  }
  // Note: Can we get away with this, or is it changing with AR? Do we need an upload per region?
  logger.debug("Customer is uploading code for GCFv2");
  const result = await gcfV2.generateUploadUrl(context.projectId, GCP_REGION);
  context.storageSource = result.storageSource;
  const apiUploadUrl = result.uploadUrl.replace("https://storage.googleapis.com", "");
  await gcs.upload(context.functionsSource, apiUploadUrl);
}

/**
 * The "deploy" stage for Cloud Functions -- uploads source code to a generated URL.
 * @param context The deploy context.
 * @param options The command-wide options object.
 * @param payload The deploy payload.
 */
export async function deploy(
  context: args.Context,
  options: args.Options,
  payload: args.Payload
): Promise<void> {
  if (!options.config.get("functions")) {
    return;
  }

  await checkHttpIam(context, options, payload);

  if (!context.functionsSource) {
    return;
  }

  try {
    await Promise.all([uploadSourceV1(context), uploadSourceV2(context)]);
    logSuccess(
      clc.green.bold("functions:") +
        " " +
        clc.bold(options.config.get("functions.source")) +
        " folder uploaded successfully"
    );
  } catch (err) {
    logWarning(clc.yellow("functions:") + " Upload Error: " + err.message);
    throw err;
  }
}
