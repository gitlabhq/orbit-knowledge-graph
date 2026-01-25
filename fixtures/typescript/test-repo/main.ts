// @ts-nocheck

import { Authentication } from './lib/authentication';
import { UserManagement } from './lib/user_management';
import { UserModel } from './app/models/user_model';

class Application {
  private users: UserModel[] = [];

  constructor() {
    this.setupAuthentication();
  }

  run(): void {
    console.log("Starting Knowledge Graph Test Application");
    
    this.createSampleUsers();
    this.testAuthentication();
    this.testTokenManagement();
    this.testAuthenticationProviders();
    
    console.log("Application completed successfully!");
  }

  private setupAuthentication(): void {
    Authentication.configureProvider('ldap', {
      host: 'ldap.example.com',
      port: 389,
      baseDn: 'dc=example,dc=com'
    });

    Authentication.configureProvider('oauth', {
      clientId: 'test_client_id',
      clientSecret: 'test_client_secret'
    });
  }

  private createSampleUsers(): void {
    console.log("\n=== Creating Sample Users ===");
    
    const userData = [
      { username: 'alice', email: 'alice@example.com', firstName: 'Alice', lastName: 'Smith' },
      { username: 'bob', email: 'bob@example.com', firstName: 'Bob', lastName: 'Johnson' },
      { username: 'charlie', email: 'charlie@example.com', firstName: 'Charlie', lastName: 'Brown' }
    ];

    for (const data of userData) {
      try {
        const user = UserModel.create(data);
        this.users.push(user);
        console.log(`Created user: ${user.displayName} (${user.username})`);
      } catch (error) {
        console.error(`Failed to create user ${data.username}: ${error}`);
      }
    }
  }

  private testAuthentication(): void {
    console.log("\n=== Testing Authentication ===");
    
    // Create a user in UserManagement for authentication
    try {
      UserManagement.createUser({
        username: 'testuser',
        email: 'test@example.com',
        password: 'testpassword123'
      });
      
      const authResult = Authentication.authenticateUser('testuser', 'testpassword123');
      console.log(`Authentication result: ${authResult}`);
    } catch (error) {
      console.error(`Authentication test failed: ${error}`);
    }
  }

  private testTokenManagement(): void {
    console.log("\n=== Testing Token Management ===");
    
    const userId = 'test-user-123';
    const session = Authentication.createSession(userId);
    
    console.log(`Created session for user ${userId}`);
    console.log(`Access token expires at: ${session.accessToken.expiresAt}`);
    console.log(`Refresh token expires at: ${session.refreshToken.expiresAt}`);
    
    // Test token validation
    const isValid = Authentication.validateToken(session.accessToken.value);
    console.log(`Token validation result: ${isValid ? 'Valid' : 'Invalid'}`);
  }

  private testAuthenticationProviders(): void {
    console.log("\n=== Testing Authentication Providers ===");
    
    const ldapProvider = Authentication.getProvider('ldap');
    const oauthProvider = Authentication.getProvider('oauth');
    
    console.log(`LDAP Provider configured: ${ldapProvider ? 'Yes' : 'No'}`);
    console.log(`OAuth Provider configured: ${oauthProvider ? 'Yes' : 'No'}`);
  }
}

// Run the application
if (require.main === module) {
  const app = new Application();
  app.run();
}

export { Application }; 
