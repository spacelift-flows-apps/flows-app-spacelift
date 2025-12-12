import { AppBlockConfigField } from "@slflows/sdk/v1";

interface SpaceliftBlockInputConfigParams
  extends Pick<
    AppBlockConfigField,
    "name" | "description" | "type" | "required" | "default"
  > {
  graphqlFieldKey?: string;
}
export function defineSpaceliftInputConfig(
  params: SpaceliftBlockInputConfigParams,
): SpaceliftBlockInputConfigParams {
  return params;
}

export function mapInputConfig(
  inputConfig: Record<string, SpaceliftBlockInputConfigParams>,
): Record<string, AppBlockConfigField> {
  return Object.fromEntries(
    Object.entries(inputConfig).map(([key, value]) => [
      key,
      {
        name: value.name,
        description: value.description,
        type: value.type,
        required: value.required,
        default: value.default,
      },
    ]),
  );
}

export function mapInputsToGraphQLVariables(
  inputConfig: Record<string, SpaceliftBlockInputConfigParams>,
  eventInputConfig: Record<string, any>,
): Record<string, any> {
  return Object.fromEntries(
    Object.entries(inputConfig)
      .filter(([key]) => eventInputConfig[key] !== undefined)
      .map(([key, value]) => [
        value.graphqlFieldKey || key,
        eventInputConfig[key],
      ]),
  );
}
