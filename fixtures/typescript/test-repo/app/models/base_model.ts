import { randomUUID } from 'crypto';
import { randomUUID as myRandomUUID } from 'crypto';

export class BaseModel {
  public readonly id: string;
  public readonly createdAt: Date;
  public updatedAt: Date;
  protected attributes: Record<string, any>;

  private static storageMap = new Map<string, BaseModel[]>();

  constructor(attributes: Record<string, any> = {}) {
    this.id = attributes.id || randomUUID();
    this.createdAt = attributes.createdAt || new Date();
    this.updatedAt = attributes.updatedAt || new Date();
    this.attributes = attributes;
  }

  static find(id: string): any {
    const storage = this.getStorage();
    return storage.find(record => record.id === id);
  }

  static all(): any[] {
    return [...this.getStorage()];
  }

  static where(conditions: Record<string, any>): any[] {
    const storage = this.getStorage();
    return storage.filter(record => 
      Object.entries(conditions).every(([key, value]) => 
        (record as any)[key] === value
      )
    );
  }

  static create(attributes: Record<string, any>): any {
    const instance = new (this as any)(attributes);
    instance.save();
    return instance;
  }

  save(): this {
    this.touch();
    if (this.persisted()) {
      this.updateInStorage();
    } else {
      this.addToStorage();
    }
    return this;
  }

  update(attributes: Record<string, any>): this {
    Object.assign(this.attributes, attributes);
    Object.entries(attributes).forEach(([key, value]) => {
      if (key in this) {
        (this as any)[key] = value;
      }
    });
    return this.save();
  }

  destroy(): void {
    const storage = (this.constructor as any).getStorage();
    const index = storage.findIndex((record: BaseModel) => record.id === this.id);
    if (index !== -1) {
      storage.splice(index, 1);
    }
    Object.freeze(this);
  }

  persisted(): boolean {
    const storage = (this.constructor as any).getStorage();
    return storage.some((record: BaseModel) => record.id === this.id);
  }

  toObject(): Record<string, any> {
    const result: Record<string, any> = {};
    Object.getOwnPropertyNames(this).forEach(key => {
      if (!key.startsWith('_')) {
        result[key.replace(/^@/, '')] = (this as any)[key];
      }
    });
    return result;
  }

  private touch(): void {
    this.updatedAt = new Date();
  }

  protected static getStorage(): BaseModel[] {
    const className = this.name;
    if (!BaseModel.storageMap.has(className)) {
      BaseModel.storageMap.set(className, []);
    }
    return BaseModel.storageMap.get(className)!;
  }

  private addToStorage(): void {
    const storage = (this.constructor as any).getStorage();
    storage.push(this);
  }

  private updateInStorage(): void {
    const storage = (this.constructor as any).getStorage();
    const index = storage.findIndex((record: BaseModel) => record.id === this.id);
    if (index !== -1) {
      storage[index] = this;
    }
  }
}
