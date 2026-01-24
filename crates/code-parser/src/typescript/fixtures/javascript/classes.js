// =============================================================================
// BASIC CLASSES
// =============================================================================

class Basic {
    constructor(value) { this.value = value; }
    method() { return this.value; }
}

class Empty {}

class OneLiner { constructor() {} method() {} }

// =============================================================================
// INHERITANCE COMPLEXITY
// =============================================================================

class Child extends Parent {
    constructor(...args) {
        super(...args);
        this.childProp = true;
    }
}

class DeepInheritance extends Very.Deeply.Nested.BaseClass {}

class ExpressionExtends extends (condition ? ClassA : ClassB) {
    constructor() { super(); }
}

// =============================================================================
// METHOD VARIATIONS
// =============================================================================

class MethodMadness {
    // Regular method
    regularMethod() {}
    
    // Async method
    async asyncMethod() { return await this.getData(); }
    
    // Generator method
    *generatorMethod() { yield this.value; }
    
    // Async generator
    async *asyncGeneratorMethod() { yield await this.fetchData(); }
    
    // Static methods
    static staticMethod() {}
    static async staticAsync() {}
    static *staticGenerator() {}
    
    // Getters/setters
    get value() { return this._value; }
    set value(v) { this._value = v; }
    
    // Computed property names
    [Symbol.iterator]() {}
    [`method${Math.random()}`]() {}
    [expression + 'Method']() {}
    
    // Private methods (modern JS)
    #privateMethod() { return 'private'; }
    static #staticPrivate() {}
    
    // Method with complex parameters
    complexMethod({a, b = 2}, [c, ...d], ...rest) {}
}

// =============================================================================
// FIELD DECLARATIONS
// =============================================================================

class FieldStress {
    // Public fields
    publicField = 'value';
    computedField = this.calculate();
    arrowMethod = () => this.publicField;
    
    // Private fields
    #privateField = 'secret';
    #privateMethod = () => this.#privateField;
    
    // Static fields
    static staticField = 'static';
    static #staticPrivate = 'private static';
    
    // Complex initializers
    complexField = users.map(u => ({...u, active: true}));
    conditionalField = condition ? valueA : valueB;
}

// =============================================================================
// DECORATOR STRESS (EXPERIMENTAL)
// =============================================================================

@classDecorator
@decoratorWithParams({option: true})
class DecoratedClass {
    @methodDecorator
    @asyncDecorator
    async decoratedMethod() {}
    
    @fieldDecorator('config')
    decoratedField = 'value';
    
    @getter
    get decoratedGetter() {}
}

// =============================================================================
// MIXED SYNTAX COMPLEXITY
// =============================================================================

class SyntaxMadness {
    constructor(
        {a, b: {c}} = {},
        [d, ...e] = [],
        ...rest
    ) {
        super();
        Object.assign(this, {a, c, d, e, rest});
    }

    #privateField = 'secret';
    
    // Method with everything
    [`dynamic${Date.now()}`] = async function*({prop = this.defaultValue}, ...args) {
        yield* await this.process(prop, ...args);
    };
    
    // Getter/setter pair with complex logic
    get #privateComputed() {
        return this.#privateField?.map?.(x => x.value) || [];
    }
    
    set #privateComputed(value) {
        this.#privateField = Array.isArray(value) ? 
            value.map(v => ({value: v})) : [];
    }
}

// =============================================================================
// NESTED CLASSES
// =============================================================================

class Outer {
    static InnerStatic = class {
        method() { return 'inner static'; }
    };
    
    createInner() {
        return new (class InnerDynamic {
            constructor(outer) { this.outer = outer; }
            getOuter() { return this.outer; }
        })(this);
    }
    
    // Class expression in method
    getProcessorClass() {
        return class Processor extends BaseProcessor {
            process(data) { return super.process(data); }
        };
    }
}

// =============================================================================
// EXPRESSION CLASSES
// =============================================================================

// // Class expressions
const ClassExpression = class {
    method() { return 'expression'; }
};

const NamedClassExpression = class NamedClass {
    getName() { return 'NamedClass'; }
};

// // NOTE: NOT COVERED
// // Class in complex expressions
// const classes = {
//     A: class A {},
//     B: class extends A { method() {} }
// };

// NOTE: NOW COVERED
const ClassFactory = (base) => class extends base {
    factory() { return true; }
};

// =============================================================================
// MODERN SYNTAX COMBINATIONS
// =============================================================================

class ModernClass {
    // Static initialization block
    static {
        this.initialized = true;
        this.setup();
    }

    // Class fields (assigned functions)
    classFieldArrowFunc = () => {}
    classFieldFuncExpression = function() {}
    classFieldGeneratorFunc = function*() {}
    
    // Private fields with complex types
    #users = new Map();
    #config = {...defaultConfig, ...userConfig};
    
    // Public method using private fields
    addUser(user) {
        this.#users.set(user.id, {...user, timestamp: Date.now()});
    }
    
    // Static private method
    static #validateConfig(config) {
        return Object.keys(config).every(key => key in allowedKeys);
    }

    static regularStaticMethod() {}
}
