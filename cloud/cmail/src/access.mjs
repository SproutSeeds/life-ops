import { createRemoteJWKSet, jwtVerify } from 'jose';

export function accessConfiguration(env) {
  const team = String(env.CMAIL_ACCESS_TEAM_DOMAIN || '').trim();
  const audience = String(env.CMAIL_ACCESS_AUD || '').trim();
  const owner = String(env.CMAIL_OWNER_EMAIL || '').trim().toLowerCase();
  if (!/^[a-z0-9-]+\.cloudflareaccess\.com$/.test(team) || !audience || !owner.includes('@')) {
    return null;
  }
  return { issuer: `https://${team}`, audience, owner };
}

export function createAuthorizer(keySetFactory = createRemoteJWKSet) {
  const keySets = new Map();
  return async function authorize(request, env) {
    const config = accessConfiguration(env);
    if (!config) return { ok: false, status: 503, error: 'sign_in_not_configured' };
    const token = request.headers.get('Cf-Access-Jwt-Assertion');
    if (!token) return { ok: false, status: 401, error: 'sign_in_required' };
    try {
      if (!keySets.has(config.issuer)) {
        keySets.set(config.issuer, keySetFactory(new URL(`${config.issuer}/cdn-cgi/access/certs`)));
      }
      const { payload } = await jwtVerify(token, keySets.get(config.issuer), {
        algorithms: ['RS256'],
        issuer: config.issuer,
        audience: config.audience,
        requiredClaims: ['exp', 'iat', 'sub', 'email'],
        clockTolerance: 5,
      });
      if (String(payload.email || '').toLowerCase() !== config.owner) {
        return { ok: false, status: 403, error: 'account_not_allowed' };
      }
      return { ok: true, email: config.owner };
    } catch {
      return { ok: false, status: 401, error: 'invalid_session' };
    }
  };
}

export const authorize = createAuthorizer();
