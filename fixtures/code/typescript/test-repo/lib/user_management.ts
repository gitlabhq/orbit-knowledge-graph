import { createHash, randomUUID } from 'crypto';

export namespace UserManagement {
  export class User {
    public readonly id: string;
    public readonly username: string;
    public readonly email: string;
    public readonly createdAt: Date;
    public active: boolean;
    private passwordHash: string;

    private static usersDb: User[] = [];

    constructor({ username, email, password }: { username: string; email: string; password: string }) {
      this.id = randomUUID();
      this.username = username;
      this.email = email;
      this.passwordHash = this.hashPassword(password);
      this.createdAt = new Date();
      this.active = true;
    }

    static findByUsername(username: string): User | undefined {
      return this.usersDb.find(user => user.username === username);
    }

    static findByEmail(email: string): User | undefined {
      return this.usersDb.find(user => user.email === email);
    }

    static create({ username, email, password }: { username: string; email: string; password: string }): User {
      const user = new User({ username, email, password });
      this.usersDb.push(user);
      return user;
    }

    verifyPassword(password: string): boolean {
      return this.hashPassword(password) === this.passwordHash;
    }

    updatePassword(newPassword: string): void {
      this.passwordHash = this.hashPassword(newPassword);
    }

    deactivate(): void {
      this.active = false;
    }

    activate(): void {
      this.active = true;
    }

    toObject(): object {
      return {
        id: this.id,
        username: this.username,
        email: this.email,
        active: this.active,
        createdAt: this.createdAt
      };
    }

    private hashPassword(password: string): string {
      return createHash('sha256').update(`${password}${this.username}`).digest('hex');
    }

    static getUsersDb(): User[] {
      return this.usersDb;
    }
  }

  export class UserRepository {
    static allUsers(): User[] {
      return User.getUsersDb();
    }

    static activeUsers(): User[] {
      return this.allUsers().filter(user => user.active);
    }

    static inactiveUsers(): User[] {
      return this.allUsers().filter(user => !user.active);
    }

    static count(): number {
      return this.allUsers().length;
    }
  }

  export function createUser({ username, email, password }: { username: string; email: string; password: string }): User {
    // Validation
    if (!username || username.trim().length === 0) {
      throw new Error("Username cannot be empty");
    }
    if (!email || email.trim().length === 0) {
      throw new Error("Email cannot be empty");
    }
    if (password.length < 8) {
      throw new Error("Password must be at least 8 characters");
    }

    // Check for existing user
    const existingUser = User.findByUsername(username) || User.findByEmail(email);
    if (existingUser) {
      throw new Error("User already exists");
    }

    return User.create({ username, email, password });
  }

  export function authenticate(username: string, password: string): User | null {
    const user = User.findByUsername(username);
    if (!user || !user.active) return null;
    
    return user.verifyPassword(password) ? user : null;
  }
}
