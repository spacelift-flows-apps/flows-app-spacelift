import { kv } from "@slflows/sdk/v1";

interface SpaceliftCredentials {
  apiKeyId: string;
  apiKeySecret: string;
  endpoint: string;
}

interface SpaceliftApiResponse<T = any> {
  data?: T;
  errors?: Array<{ message: string; extensions?: Record<string, string> }>;
}

interface CachedJWTToken {
  jwt: string;
}

function getCacheKey(credentials: SpaceliftCredentials): string {
  return `spacelift_jwt_${credentials.endpoint}_${credentials.apiKeyId}`;
}

export function extractCredentials(
  appConfig: Record<string, any>,
): SpaceliftCredentials {
  if (!appConfig.apiKeyId || !appConfig.apiKeySecret || !appConfig.endpoint) {
    throw new Error("Missing required Spacelift credentials in app config");
  }

  return {
    apiKeyId: appConfig.apiKeyId,
    apiKeySecret: appConfig.apiKeySecret,
    endpoint: appConfig.endpoint,
  };
}

async function fetchNewJWT(
  credentials: SpaceliftCredentials,
): Promise<{ jwt: string; validUntil: number }> {
  const response = await fetch(`https://${credentials.endpoint}/graphql`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `
        mutation GetSpaceliftToken($id: ID!, $secret: String!) {
          apiKeyUser(id: $id, secret: $secret) {
            jwt
            validUntil
          }
        }
      `,
      variables: {
        id: credentials.apiKeyId,
        secret: credentials.apiKeySecret,
      },
    }),
  });

  const result: SpaceliftApiResponse<{
    apiKeyUser: { jwt: string; validUntil: number };
  }> = await response.json();

  if (result.errors) {
    throw new Error(
      `Authentication failed: ${result.errors.map((e) => e.message).join(", ")}`,
    );
  }

  if (!result.data?.apiKeyUser?.jwt || !result.data?.apiKeyUser?.validUntil) {
    throw new Error("Failed to obtain JWT token or expiration time");
  }

  return {
    jwt: result.data.apiKeyUser.jwt,
    validUntil: result.data.apiKeyUser.validUntil,
  };
}

export async function refreshSpaceliftJWT(
  credentials: SpaceliftCredentials,
): Promise<string> {
  const cacheKey = getCacheKey(credentials);

  const { jwt, validUntil } = await fetchNewJWT(credentials);

  // Cache the new token with TTL (5 minute safety buffer)
  const now = Math.floor(Date.now() / 1000);
  const ttlSeconds = Math.max(validUntil - now - 300, 60); // At least 1 minute TTL

  await kv.app.set({
    key: cacheKey,
    value: {
      jwt,
    } as CachedJWTToken,
    ttl: ttlSeconds,
  });

  return jwt;
}

export async function getSpaceliftJWT(
  credentials: SpaceliftCredentials,
): Promise<string> {
  const cacheKey = getCacheKey(credentials);

  const cachedToken = await kv.app.get(cacheKey);

  if (cachedToken.value) {
    const cached = cachedToken.value as CachedJWTToken;
    return cached.jwt;
  }

  return refreshSpaceliftJWT(credentials);
}

function formatExtensions(extensions: Record<string, string>) {
  if (typeof extensions !== "object" || extensions === null) {
    return JSON.stringify(extensions);
  }

  return Object.entries(extensions)
    .map(([key, value], index) => `    ${index + 1}. ${key}: ${value}`)
    .join("\n");
}

export async function executeSpaceliftQuery<T = any>(
  credentials: SpaceliftCredentials,
  query: string,
  variables?: Record<string, any>,
): Promise<T> {
  let jwt = await getSpaceliftJWT(credentials);

  const makeRequest = async (token: string) => {
    const response = await fetch(`https://${credentials.endpoint}/graphql`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        query,
        variables,
      }),
    });

    return await response.json();
  };

  let result: SpaceliftApiResponse<T> = await makeRequest(jwt);

  if (result.errors) {
    const errorMessage = result.errors
      .map((e) => e.message)
      .join(" ")
      .toLowerCase();

    if (errorMessage.includes("unauthorized")) {
      // Refresh JWT and retry once
      jwt = await refreshSpaceliftJWT(credentials);
      result = await makeRequest(jwt);

      // Check for errors again after retry
      if (result.errors) {
        throw new Error(
          `GraphQL error:\n${result.errors.map((e) => (e.extensions ? ` - ${e.message}:\n${formatExtensions(e.extensions)}` : ` - ${e.message}`)).join("\n")}\n`,
        );
      }
    } else {
      throw new Error(
        `GraphQL error:\n${result.errors.map((e) => (e.extensions ? ` - ${e.message}:\n${formatExtensions(e.extensions)}` : ` - ${e.message}`)).join("\n")}\n`,
      );
    }
  }

  if (!result.data) {
    throw new Error("No data returned from Spacelift API");
  }

  return result.data;
}
