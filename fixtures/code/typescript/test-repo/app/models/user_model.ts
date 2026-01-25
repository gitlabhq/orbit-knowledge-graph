import { BaseModel } from './base_model';

export class UserModel extends BaseModel {
  public username?: string;
  public email?: string;
  public firstName?: string;
  public lastName?: string;
  public active: boolean;
  private errors: string[] = [];

  constructor(attributes: Record<string, any> = {}) {
    super(attributes);
    this.username = attributes.username;
    this.email = attributes.email;
    this.firstName = attributes.firstName;
    this.lastName = attributes.lastName;
    this.active = attributes.active !== undefined ? attributes.active : true;
  }

  static findByUsername(username: string): UserModel | undefined {
    return this.all().find((user: UserModel) => user.username === username);
  }

  static findByEmail(email: string): UserModel | undefined {
    return this.all().find((user: UserModel) => user.email === email);
  }

  static activeUsers(): UserModel[] {
    return this.where({ active: true });
  }

  static inactiveUsers(): UserModel[] {
    return this.where({ active: false });
  }

  get fullName(): string {
    return `${this.firstName || ''} ${this.lastName || ''}`.trim();
  }

  get displayName(): string {
    return this.fullName || this.username || '';
  }

  activate(): this {
    return this.update({ active: true });
  }

  deactivate(): this {
    return this.update({ active: false });
  }

  changeEmail(newEmail: string): this {
    if (UserModel.findByEmail(newEmail)) {
      throw new Error("Email already taken");
    }
    return this.update({ email: newEmail });
  }

  changeUsername(newUsername: string): this {
    if (UserModel.findByUsername(newUsername)) {
      throw new Error("Username already taken");
    }
    return this.update({ username: newUsername });
  }

  toObject(): Record<string, any> {
    return {
      ...super.toObject(),
      username: this.username,
      email: this.email,
      firstName: this.firstName,
      lastName: this.lastName,
      active: this.active,
      fullName: this.fullName,
      displayName: this.displayName
    };
  }

  valid(): boolean {
    this.errors = [];
    return this.validateUsername() && this.validateEmail();
  }

  getErrors(): string[] {
    return [...this.errors];
  }

  private validateUsername(): boolean {
    if (!this.username || this.username.trim().length === 0) {
      this.errors.push("Username cannot be empty");
      return false;
    }
    
    if (this.username.length < 3) {
      this.errors.push("Username must be at least 3 characters");
      return false;
    }
    
    return true;
  }

  private validateEmail(): boolean {
    if (!this.email || this.email.trim().length === 0) {
      this.errors.push("Email cannot be empty");
      return false;
    }
    
    const emailRegex = /^[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+$/i;
    if (!emailRegex.test(this.email)) {
      this.errors.push("Email format is invalid");
      return false;
    }
    
    return true;
  }
}
