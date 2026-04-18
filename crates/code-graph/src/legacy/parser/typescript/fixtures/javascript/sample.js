
// JWT Authentication Service - Complex JavaScript Test File
'use strict';

const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const { promisify } = require('util');

/**
 * Base authentication controller class
 */
class BaseAuthController {
    constructor(config = {}) {
        this.config = {
            tokenExpiry: '1h',
            issuer: 'gitlab-auth',
            audience: 'container-registry',
            ...config
        };
        this.services = new Map();
        this._initializeServices();
    }

    // Private method to initialize services
    _initializeServices() {
        this.services.set('container_registry', new ContainerRegistryAuthService());
        this.services.set('dependency_proxy', new DependencyProxyAuthService());
        this.services.set('package_registry', new PackageRegistryAuthService());
    }

    // Abstract method - to be overridden
    authenticate() {
        throw new Error('authenticate method must be implemented by subclass');
    }

    // Static method for token validation
    static validateTokenStructure(token) {
        const parts = token.split('.');
        return parts.length === 3;
    }
}

/**
 * JWT Controller extending base controller
 */
class JwtController extends BaseAuthController {
    constructor(options) {
        super(options);
        this.SERVICES = Object.freeze({
            CONTAINER_REGISTRY: 'container_registry_auth',
            DEPENDENCY_PROXY: 'dependency_proxy_auth',
            PACKAGE_REGISTRY: 'package_registry_auth'
        });

        // Instance variables
        this.authenticationResult = null;
        this.rawToken = null;
        this.currentUser = null;

        // Bind methods to preserve context
        this.auth = this.auth.bind(this);
        this.authenticateProjectOrUser = this.authenticateProjectOrUser.bind(this);
        this.renderAccessDenied = this.renderAccessDenied.bind(this);
    }

    // Main authentication endpoint
    async auth(req, res, next) {
        try {
            const serviceName = req.params.service || req.query.service;
            const service = this.services.get(serviceName);

            if (!service) {
                return res.status(404).json({ error: 'Service not found' });
            }

            // Authenticate user or project
            await this.authenticateProjectOrUser(req);

            if (this.authenticationResult?.failed) {
                return this.renderAccessDenied(res);
            }

            // Execute authentication service
            const result = await service.execute({
                project: this.authenticationResult.project,
                user: this.authUser,
                params: this.getAuthParams(req),
                abilities: this.authenticationResult.authenticationAbilities
            });

            res.status(result.httpStatus || 200).json(result);
        } catch (error) {
            this.logAuthenticationFailed(req.body?.login || 'unknown', error);
            next(error);
        }
    }

    // Private authentication logic
    async authenticateProjectOrUser(request) {
        // Initialize default authentication result
        this.authenticationResult = {
            user: null,
            project: null,
            type: 'none',
            authenticationAbilities: ['read_repository'],
            failed: false
        };

        const authHeader = request.headers.authorization;
        if (!authHeader || !authHeader.startsWith('Basic ')) {
            this.authenticationResult.failed = true;
            return;
        }

        try {
            const credentials = this.parseBasicAuth(authHeader);
            const result = await this.findForGitClient(
                credentials.username,
                credentials.password,
                { project: null, request }
            );

            Object.assign(this.authenticationResult, result);

            if (result.type === 'personal_access_token') {
                this.rawToken = credentials.password;
            }

        } catch (error) {
            if (error.name === 'MissingPersonalAccessTokenError') {
                this.authenticationResult.failed = true;
            } else {
                throw error;
            }
        }
    }

    // Helper method for parsing basic auth
    parseBasicAuth(authHeader) {
        const encoded = authHeader.slice('Basic '.length);
        const decoded = Buffer.from(encoded, 'base64').toString('utf-8');
        const [username, password] = decoded.split(':');
        return { username, password };
    }

    // Mock git client authentication
    async findForGitClient(login, password, options = {}) {
        // Simulate async authentication logic
        await new Promise(resolve => setTimeout(resolve, 100));

        if (password.startsWith('glpat-')) {
            return {
                user: { id: 123, username: login },
                project: options.project,
                type: 'personal_access_token',
                authenticationAbilities: ['read_repository', 'write_repository']
            };
        }

        if (password.startsWith('gldt-')) {
            return {
                user: null,
                project: { id: 456, name: 'test-project' },
                type: 'deploy_token',
                authenticationAbilities: ['read_repository']
            };
        }

        return {
            user: null,
            project: null,
            type: 'none',
            failed: true
        };
    }

    // Logging failed authentication attempts
    logAuthenticationFailed(login, result) {
        const logInfo = {
            message: 'JWT authentication failed',
            httpUser: login,
            remoteIp: result.request?.ip || 'unknown',
            authService: result.service,
            'authResult.type': result.type || 'unknown',
            'authResult.actorType': result.user?.constructor.name || 'none',
            timestamp: new Date().toISOString()
        };

        console.warn('Auth Failed:', JSON.stringify(logInfo, null, 2));
    }

    // Render access denied response
    renderAccessDenied(res) {
        const helpPageUrl = 'https://docs.gitlab.com/ee/user/profile/account/two_factor_authentication_troubleshooting.html';

        const errorResponse = {
            errors: [{
                code: 'UNAUTHORIZED',
                message: `HTTP Basic: Access denied. If a password was provided for Git authentication, ` +
                    `the password was incorrect or you're required to use a token instead of a password. ` +
                    `If a token was provided, it was either incorrect, expired, or improperly scoped. ` +
                    `See ${helpPageUrl} for more information.`
            }]
        };

        return res.status(401).json(errorResponse);
    }

    // Extract authentication parameters
    getAuthParams(request) {
        const { service, account, client_id } = request.query;
        const baseParams = { service, account, client_id };

        return {
            ...baseParams,
            ...this.getAdditionalParams(request)
        };
    }

    // Additional authentication parameters
    getAdditionalParams(request) {
        const additionalParams = {
            scopes: this.getScopesParam(request),
            rawToken: this.rawToken,
            deployToken: this.authenticationResult?.deployToken,
            authType: this.authenticationResult?.type
        };

        // Remove undefined values
        return Object.fromEntries(
            Object.entries(additionalParams).filter(([_, value]) => value !== undefined)
        );
    }

    // Parse scopes parameter (handles space-delimited and multiple scope params)
    getScopesParam(request) {
        const { scope } = request.query;
        if (!scope) return undefined;

        const scopes = Array.isArray(scope) ? scope : [scope];
        return scopes.flatMap(s => s.split(/\s+/)).filter(Boolean);
    }

    // Getter for authenticated user
    get authUser() {
        if (!this._memoizedAuthUser) {
            this._memoizedAuthUser = this.authenticationResult?.user || null;
        }
        return this._memoizedAuthUser;
    }

    // Admin mode bypass (arrow function)
    bypassAdminMode = async (callback) => {
        const currentSettings = require('./config/settings');
        if (!currentSettings.adminMode) {
            return await callback();
        }

        const userId = this.authUser?.id;
        if (!userId) return await callback();

        // Simulate bypassing admin mode
        console.log(`Bypassing admin mode for user ${userId}`);
        const result = await callback();
        console.log(`Admin mode bypass completed for user ${userId}`);

        return result;
    }
}

/**
 * Container Registry Authentication Service
 */
class ContainerRegistryAuthService {
    constructor() {
        this.audience = 'harbor-registry';
        this.issuer = 'gitlab-jwt';
    }

    async execute(options) {
        const { user, project, params, abilities } = options;

        if (!this.hasRequiredAbilities(abilities)) {
            return { error: 'Insufficient permissions', httpStatus: 403 };
        }

        const token = await this.generateToken(user, project, params);

        return {
            token,
            access_token: token,
            expires_in: 3600,
            issued_at: new Date().toISOString()
        };
    }

    hasRequiredAbilities(abilities = []) {
        const required = ['read_repository'];
        return required.every(ability => abilities.includes(ability));
    }

    async generateToken(user, project, params) {
        const payload = {
            iss: this.issuer,
            aud: this.audience,
            exp: Math.floor(Date.now() / 1000) + 3600,
            iat: Math.floor(Date.now() / 1000),
            sub: user?.username || 'anonymous',
            access: this.buildAccessClaims(user, project, params)
        };

        return jwt.sign(payload, process.env.JWT_SECRET || 'secret');
    }

    buildAccessClaims(user, project, params) {
        const scopes = params.scopes || [];
        return scopes.map(scope => ({
            type: 'repository',
            name: `${project?.path || 'library'}/image`,
            actions: this.parseActions(scope)
        }));
    }

    parseActions(scope) {
        if (scope.includes('push')) return ['pull', 'push'];
        if (scope.includes('pull')) return ['pull'];
        return ['pull'];
    }
}

/**
 * Dependency Proxy Authentication Service  
 */
class DependencyProxyAuthService extends ContainerRegistryAuthService {
    constructor() {
        super();
        this.audience = 'dependency-proxy';
    }

    buildAccessClaims(user, project, params) {
        return [{
            type: 'repository',
            name: `${project?.path || 'group'}/dependency_proxy/*`,
            actions: ['pull']
        }];
    }
}

/**
 * Package Registry Authentication Service
 */
class PackageRegistryAuthService {
    async execute(options) {
        const { user, project, params } = options;

        // Different token structure for package registry
        const token = this.createPackageToken(user, project);

        return {
            token,
            token_type: 'Bearer',
            expires_in: 7200
        };
    }

    createPackageToken(user, project) {
        const claims = {
            user_id: user?.id,
            project_id: project?.id,
            scope: 'package:read package:write',
            exp: Date.now() + (7200 * 1000)
        };

        return Buffer.from(JSON.stringify(claims)).toString('base64');
    }
}

// Factory function for creating authentication services
function createAuthService(type, config = {}) {
    const services = {
        jwt: () => new JwtController(config),
        container: () => new ContainerRegistryAuthService(),
        dependency: () => new DependencyProxyAuthService(),
        package: () => new PackageRegistryAuthService()
    };

    const factory = services[type];
    if (!factory) {
        throw new Error(`Unknown service type: ${type}`);
    }

    return factory();
}

// Utility functions (various assignment patterns)
const tokenUtils = {
    // Object method shorthand
    validateExpiry(token) {
        try {
            const decoded = jwt.decode(token);
            return decoded.exp > Date.now() / 1000;
        } catch {
            return false;
        }
    },

    // Arrow function assignment
    extractClaims: (token) => {
        try {
            return jwt.decode(token, { complete: true });
        } catch (error) {
            console.error('Token decode error:', error);
            return null;
        }
    }
};

// Destructuring assignments
const { validateExpiry, extractClaims } = tokenUtils;
const [primaryService, fallbackService] = ['container_registry', 'dependency_proxy'];

// Complex assignment with computed property
const serviceConfig = {
    [primaryService]: { timeout: 5000, retries: 3 },
    [fallbackService]: { timeout: 3000, retries: 1 }
};

// Array destructuring with rest
const [mainAudience, ...otherAudiences] = ['harbor-registry', 'dependency-proxy', 'package-registry'];

// Export patterns
module.exports = {
    JwtController,
    BaseAuthController,
    ContainerRegistryAuthService,
    DependencyProxyAuthService,
    PackageRegistryAuthService,
    createAuthService,
    tokenUtils,
    serviceConfig
};

// Additional exports with different patterns
exports.VERSION = '1.0.0';
exports.DEFAULT_CONFIG = Object.freeze({
    tokenExpiry: 3600,
    maxRetries: 3,
    supportedServices: ['container_registry', 'dependency_proxy', 'package_registry']
});

// Global assignments (testing various assignment types)
global.AUTH_CONSTANTS = {
    TOKEN_TYPES: {
        PERSONAL: 'personal_access_token',
        DEPLOY: 'deploy_token',
        PROJECT: 'project_token'
    }
};

// Prototype extension
String.prototype.isValidToken = function () {
    return this.startsWith('glpat-') || this.startsWith('gldt-');
};

// Complex nested object assignment
const authMiddleware = {
    jwt: {
        verify: async (token, options = {}) => {
            const { secret = process.env.JWT_SECRET, ...jwtOptions } = options;
            return promisify(jwt.verify)(token, secret, jwtOptions);
        }
    },

    basic: {
        parse: (header) => {
            if (!header?.startsWith('Basic ')) return null;
            const encoded = header.slice(6);
            const decoded = Buffer.from(encoded, 'base64').toString();
            const [username, password] = decoded.split(':');
            return { username, password };
        }
    }
};

// Assignment with side effects
let requestCounter = 0;
const incrementCounter = () => ++requestCounter;
const currentCount = incrementCounter();

// Conditional assignment
const environment = process.env.NODE_ENV || 'development';
const isDevelopment = environment === 'development';
const logLevel = isDevelopment ? 'debug' : 'info';

// Assignment in try-catch
let databaseConnection;
try {
    databaseConnection = require('./database').connect();
} catch (error) {
    databaseConnection = null;
    console.warn('Database connection failed, using mock data');
}

// Final export with computed properties
const dynamicExports = {};
['Controller', 'Service', 'Middleware'].forEach(suffix => {
    dynamicExports[`create${suffix}`] = (config) => {
        return createAuthService('jwt', config);
    };
});

Object.assign(module.exports, dynamicExports);
