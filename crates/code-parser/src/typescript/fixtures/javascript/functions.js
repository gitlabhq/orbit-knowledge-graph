
// =============================================================================
// FUNCTION EXPRESSIONS - Anonymous
// =============================================================================

// Basic anonymous function expression
const anonymousBasic = function () {
    return "anonymous";
};

// Anonymous function with parameters
const anonymousWithParams = function (a, b) {
    return a + b;
};

// Anonymous function with complex parameters
const anonymousComplex = function ({ a }, [b], ...rest) {
    return "complex anonymous";
};

// =============================================================================
// FUNCTION EXPRESSIONS - Named
// =============================================================================

// Named function expression
const namedExpression = function namedFunc() {
    return "named expression";
};

// Named function expression with parameters
const namedWithParams = function namedParamFunc(a, b, c) {
    return "named with params";
};

// Named function expression with complex parameters
const namedComplex = function complexNamed({ a = 1 }, [b, ...c] = [], ...rest) {
    return "named complex";
};

// =============================================================================
// ARROW FUNCTIONS - Basic Cases
// =============================================================================

// Simple arrow function
const simpleArrow = () => "simple";

// Arrow function with single parameter (no parentheses)
const singleParam = x => x * 2;

// Arrow function with multiple parameters
const multipleParams = (a, b) => a + b;

// Arrow function with block body
const blockBody = (x) => {
    const result = x * 2;
    return result;
};

// =============================================================================
// ARROW FUNCTIONS - Parameter Edge Cases
// =============================================================================

// Arrow function with default parameters
const arrowDefaults = (a = 1, b = 2) => a + b;

// Arrow function with rest parameters
const arrowRest = (...args) => args.length;

// Arrow function with destructured parameters
const arrowDestructure = ({ a, b }) => a + b;

// Arrow function with array destructuring
const arrowArrayDestructure = ([first, second]) => first + second;

// Arrow function with complex destructuring
const arrowComplexDestructure = ({ a: { b } }, [c, ...d]) => b + c;

// Arrow function with mixed parameter types
const arrowMixed = (a, { b = 2 }, [c], ...rest) => "mixed";

// =============================================================================
// ARROW FUNCTIONS - Return Value Edge Cases
// =============================================================================

// Arrow function returning object literal (needs parentheses)
const returnObject = () => ({ key: "value", number: 42 });

// Arrow function returning array
const returnArray = () => [1, 2, 3];

// Arrow function returning function
const returnFunction = () => () => "nested";

// Arrow function with conditional return
const conditionalReturn = (x) => x > 0 ? "positive" : "negative";

// Arrow function with complex expression
const complexExpression = (a, b) => a.map(x => x * b).filter(x => x > 10);

// =============================================================================
// NESTED AND HIGHER-ORDER FUNCTIONS
// =============================================================================

// Function returning function
function outerFunction() {
    return function innerFunction() {
        return "inner";
    };
}

// Arrow function returning arrow function
const outerArrow = () => () => "nested arrow";

// Mixed nesting
const mixedNesting = function () {
    return (x) => {
        return function (y) {
            return x + y;
        };
    };
};

// Function with nested function declarations
function withNestedDeclaration() {
    function nested() {
        return "nested declaration";
    }
    return nested();
}

// =============================================================================
// ASYNC FUNCTIONS
// =============================================================================

// Async function declaration
async function asyncDeclaration() {
    return await Promise.resolve("async");
}

// Async function expression
const asyncExpression = async function () {
    return await fetch("/api/data");
};

// Async arrow function
const asyncArrow = async () => {
    return await Promise.resolve("async arrow");
};

// Async arrow function with parameters
const asyncArrowParams = async (url, options) => {
    return await fetch(url, options);
};

// =============================================================================
// GENERATOR FUNCTIONS
// =============================================================================

// Generator function declaration
function* generatorDeclaration() {
    yield 1;
    yield 2;
}

// Generator function expression
const generatorExpression = function* () {
    yield* [1, 2, 3];
};

// Async generator
async function* asyncGenerator() {
    yield await Promise.resolve(1);
}

// =============================================================================
// METHOD DEFINITIONS IN OBJECTS
// =============================================================================

const objectWithMethods = {
    // Method shorthand
    methodShorthand() {
        return "shorthand";
    },

    // Traditional method
    traditionalMethod: function () {
        return "traditional";
    },

    // Arrow method (lexical this)
    arrowMethod: () => {
        return "arrow method";
    },

    // Async method
    async asyncMethod() {
        return await Promise.resolve("async method");
    },

    // Generator method
    *generatorMethod() {
        yield "generator method";
    },

    // Computed property name method
    [`computed${Math.random()}`]() {
        return "computed";
    }
};

// =============================================================================
// IMMEDIATELY INVOKED FUNCTION EXPRESSIONS (IIFE)
// =============================================================================

// Classic IIFE
(function () {
    return "IIFE";
})();

// IIFE with parameters
(function (x, y) {
    return x + y;
})(1, 2);

// Arrow IIFE
(() => {
    return "arrow IIFE";
})();

// Named IIFE
(function namedIIFE() {
    return "named IIFE";
})();

// =============================================================================
// FUNCTIONS WITH COMPLEX WHITESPACE AND FORMATTING
// =============================================================================

// Function with no spaces
const noSpaces = () => "no spaces";

// Function with excessive spaces
const excessiveSpaces = (a, b) => a + b;

// Function split across many lines
const
    splitAcrossLines
        =
        (
            param1
            ,
            param2
        ) => {
            return
            param1
                +
                param2
                ;
        };

// Function with mixed line endings and indentation
function mixedWhitespace(
    a,
    b,
    c
) {
    return a + b + c;
}

// GETTERS AND SETTERS

class GetterSetter {
    get name() {
        return "getter";
    }
    set name(value) {
        return "setter";
    }
};
