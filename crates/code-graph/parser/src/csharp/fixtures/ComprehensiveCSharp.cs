using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Text;
using static System.Math;
using MyAlias = System.Collections.Generic.Dictionary<string, int>;

// Global using (C# 10+)
global using System.IO;

// File-scoped namespace (C# 10+)
namespace ComprehensiveLanguageFeatures;

#region Attributes
[Serializable]
[Obsolete("This is obsolete", true)]
[AttributeUsage(AttributeTargets.All, AllowMultiple = true)]
public class CustomAttribute : Attribute
{
    public string Value { get; set; }
    public CustomAttribute([CallerMemberName] string caller = "") => Value = caller;
}
#endregion

#region Interfaces
public interface IBasicInterface
{
    void Method();
    int Property { get; set; }
    event EventHandler Event;
    string this[int index] { get; set; }
}

public interface IGenericInterface<T> where T : class, new()
{
    T GetValue();
    void SetValue(T value);
}

// Interface with default implementation (C# 8+)
public interface IModernInterface
{
    void RequiredMethod();
    
    void DefaultMethod()
    {
        Console.WriteLine("Default implementation");
    }
    
    static abstract void StaticAbstractMethod();
    
    // Static virtual member (C# 11+)
    static virtual int StaticVirtualProperty => 42;
}
#endregion

#region Delegates and Events
public delegate void SimpleDelegate();
public delegate T GenericDelegate<T>(T input) where T : struct;
public delegate ref int RefDelegate(ref int value);

// Multicast delegate
public delegate void MulticastDelegate(string message);
#endregion

#region Enums
public enum SimpleEnum
{
    None = 0,
    First = 1,
    Second = 2,
    Third = 4,
    Combined = First | Second
}

[Flags]
public enum FlagsEnum : byte
{
    None = 0b_0000_0000,
    Read = 0b_0000_0001,
    Write = 0b_0000_0010,
    Execute = 0b_0000_0100,
    All = Read | Write | Execute
}
#endregion

#region Structs
public struct SimpleStruct
{
    public int X { get; set; }
    public int Y { get; set; }
    
    public SimpleStruct(int x, int y)
    {
        X = x;
        Y = y;
    }
}

// Record struct (C# 10+)
public record struct Point(double X, double Y)
{
    public double Distance => Sqrt(X * X + Y * Y);
}

// Readonly struct (C# 7.2+)
public readonly struct ImmutablePoint
{
    public double X { get; }
    public double Y { get; }
    
    public ImmutablePoint(double x, double y) => (X, Y) = (x, y);
}

// Ref struct (C# 7.2+)
public ref struct SpanWrapper
{
    private Span<int> _span;
    public SpanWrapper(Span<int> span) => _span = span;
}
#endregion

#region Classes
// Abstract base class
public abstract class AbstractBase
{
    public abstract void AbstractMethod();
    public virtual void VirtualMethod() => Console.WriteLine("Base virtual");
    protected internal abstract string AbstractProperty { get; set; }
}

// Sealed class
public sealed class SealedClass : AbstractBase
{
    public override void AbstractMethod() => Console.WriteLine("Implemented");
    protected internal override string AbstractProperty { get; set; } = "";
}

// Generic class with constraints
public class GenericClass<T, U> 
    where T : class, IComparable<T>, new()
    where U : struct, IEquatable<U>
{
    public T? GenericProperty { get; set; }
    public U StructProperty { get; set; }
    
    public void GenericMethod<V>(V value) where V : T
    {
        Console.WriteLine(value);
    }
}

// Partial class
public partial class PartialClass
{
    public partial void PartialMethod();
}

public partial class PartialClass
{
    public partial void PartialMethod() => Console.WriteLine("Partial implementation");
}

// Static class
public static class StaticClass
{
    public const int ConstField = 42;
    public static readonly DateTime StaticReadonlyField = DateTime.Now;
    
    static StaticClass()
    {
        Console.WriteLine("Static constructor");
    }
    
    public static void ExtensionMethod(this string str) => Console.WriteLine(str);
}
#endregion

#region Records (C# 9+)
public record Person(string FirstName, string LastName)
{
    public int Age { get; init; }
    
    // Positional record with inheritance
    public record Employee(string FirstName, string LastName, int Id) 
        : Person(FirstName, LastName);
}

// Record class with custom equality
public record CustomRecord
{
    public string Value { get; init; } = "";
    
    public virtual bool Equals(CustomRecord? other) => 
        other is not null && Value.Equals(other.Value, StringComparison.OrdinalIgnoreCase);
    
    public override int GetHashCode() => Value.ToLowerInvariant().GetHashCode();
}
#endregion

#region Main Feature Demonstration Class
[Custom("Main class")]
public class FeatureDemonstration : AbstractBase, IBasicInterface, IGenericInterface<string>
{
    #region Fields
    private int _privateField;
    protected string _protectedField = "";
    internal decimal _internalField;
    public static int StaticField;
    public const double ConstField = 3.14159;
    public readonly Guid ReadonlyField = Guid.NewGuid();
    private volatile int _volatileField;
    [ThreadStatic] private static int _threadStaticField;
    #endregion
    
    #region Properties
    // Auto-property
    public string AutoProperty { get; set; } = "Default";
    
    // Init-only property (C# 9+)
    public int InitOnlyProperty { get; init; }
    
    // Required property (C# 11+)
    public required string RequiredProperty { get; set; }
    
    // Property with backing field
    private string _backingField = "";
    public string PropertyWithBackingField
    {
        get => _backingField;
        set => _backingField = value ?? throw new ArgumentNullException(nameof(value));
    }
    
    // Expression-bodied property
    public string ComputedProperty => $"{AutoProperty}_{InitOnlyProperty}";
    
    // Property with different access modifiers
    public string MixedAccessProperty { get; private set; } = "";
    
    // Static property
    public static int StaticProperty { get; set; }
    #endregion
    
    #region Indexers
    private readonly Dictionary<int, string> _indexerData = new();
    
    public string this[int index]
    {
        get => _indexerData.GetValueOrDefault(index, "");
        set => _indexerData[index] = value;
    }
    
    // Multi-parameter indexer
    public string this[int x, int y] => $"[{x},{y}]";
    #endregion
    
    #region Constructors
    // Static constructor
    static FeatureDemonstration()
    {
        StaticField = 100;
    }
    
    // Primary constructor would go here in C# 12+
    
    // Default constructor
    public FeatureDemonstration()
    {
        RequiredProperty = "Required";
    }
    
    // Constructor with parameters
    public FeatureDemonstration(string requiredProperty) : this()
    {
        RequiredProperty = requiredProperty;
    }
    
    // Constructor chaining
    public FeatureDemonstration(string requiredProperty, int initOnly) : this(requiredProperty)
    {
        InitOnlyProperty = initOnly;
    }
    #endregion
    
    #region Methods
    // Override abstract method
    public override void AbstractMethod()
    {
        Console.WriteLine("Abstract method implementation");
    }
    
    // Override abstract property
    protected internal override string AbstractProperty { get; set; } = "";
    
    // Interface implementations
    public void Method() => Console.WriteLine("Interface method");
    
    public int Property { get; set; }
    
    public event EventHandler? Event;
    
    public string GetValue() => "Value";
    
    public void SetValue(string value) => AutoProperty = value;
    
    // Method with various parameter modifiers
    public void ParameterModifiers(
        int normalParam,
        ref int refParam,
        out int outParam,
        in int inParam,
        params int[] paramsArray)
    {
        outParam = normalParam + refParam + inParam + paramsArray.Sum();
    }
    
    // Generic method
    public T GenericMethod<T>(T input) where T : IComparable<T>
    {
        return input;
    }
    
    // Async method
    public async Task<int> AsyncMethod()
    {
        await Task.Delay(100);
        return 42;
    }
    
    // Async enumerable (C# 8+)
    public async IAsyncEnumerable<int> AsyncEnumerable([EnumeratorCancellation] CancellationToken ct = default)
    {
        for (int i = 0; i < 10; i++)
        {
            await Task.Delay(10, ct);
            yield return i;
        }
    }
    
    // Iterator method
    public IEnumerable<int> IteratorMethod()
    {
        yield return 1;
        yield return 2;
        yield return 3;
    }
    
    // Local function
    public int LocalFunctionExample(int x)
    {
        return LocalAdd(x, 10);
        
        static int LocalAdd(int a, int b) => a + b;
    }
    
    // Expression-bodied method
    public string ExpressionBodiedMethod() => "Expression body";
    
    // Tuple return type
    public (int sum, int product) TupleReturn(int a, int b) => (a + b, a * b);
    
    // Named arguments example
    public void NamedArguments(int x = 0, int y = 0, int z = 0)
    {
        Console.WriteLine($"x={x}, y={y}, z={z}");
    }
    
    // Operator overloading
    public static FeatureDemonstration operator +(FeatureDemonstration a, FeatureDemonstration b)
    {
        return new FeatureDemonstration { AutoProperty = a.AutoProperty + b.AutoProperty };
    }
    
    // Implicit/Explicit conversion
    public static implicit operator string(FeatureDemonstration obj) => obj.AutoProperty;
    public static explicit operator int(FeatureDemonstration obj) => obj.InitOnlyProperty;
    
    // Destructor/Finalizer
    ~FeatureDemonstration()
    {
        Console.WriteLine("Finalizer called");
    }
    #endregion
    
    #region Pattern Matching Examples
    public string PatternMatchingExamples(object obj)
    {
        // Type pattern
        if (obj is string s)
            return s;
        
        // Constant pattern
        if (obj is null)
            return "null";
        
        // var pattern
        if (obj is var x && x.GetType() == typeof(int))
            return "int";
        
        // Property pattern (C# 8+)
        if (obj is Person { FirstName: "John", Age: > 18 } person)
            return person.LastName;
        
        // Tuple pattern (C# 8+)
        if (obj is (int a, int b))
            return $"{a + b}";
        
        // Positional pattern (C# 8+)
        if (obj is Point(var px, var py))
            return $"Point at ({px}, {py})";
        
        // Switch expression (C# 8+)
        return obj switch
        {
            int n when n > 0 => "positive",
            int n when n < 0 => "negative",
            0 => "zero",
            string { Length: > 10 } => "long string",
            string str => str,
            IEnumerable<int> list => string.Join(",", list),
            null => "null",
            _ => "unknown"
        };
    }
    
    // Relational patterns (C# 9+)
    public string RelationalPatterns(int value) => value switch
    {
        < 0 => "negative",
        > 0 and <= 10 => "small positive",
        > 10 and <= 100 => "medium",
        > 100 => "large",
        _ => "zero"
    };
    
    // List patterns (C# 11+)
    public string ListPatterns(int[] array) => array switch
    {
        [] => "empty",
        [var single] => $"single: {single}",
        [var first, .. var rest, var last] => $"first: {first}, last: {last}, middle count: {rest.Length}",
        _ => "unknown pattern"
    };
    #endregion
    
    #region Modern C# Features
    // Nullable reference types (C# 8+)
    public string? NullableReference { get; set; }
    public string NonNullableReference { get; set; } = "";
    
    // Range and Index (C# 8+)
    public void RangeAndIndexExample()
    {
        var array = new[] { 1, 2, 3, 4, 5 };
        var lastItem = array[^1]; // Index from end
        var range = array[1..^1]; // Range
        var allExceptFirst = array[1..];
        var firstThree = array[..3];
    }
    
    // Using declarations (C# 8+)
    public void UsingDeclaration()
    {
        using var stream = new MemoryStream();
        // Stream is disposed at the end of the scope
    }
    
    // Target-typed new (C# 9+)
    private List<string> _list = new();
    private Dictionary<string, int> _dict = new() { ["key"] = 1 };
    
    // With expressions (C# 9+)
    public Person ModifyPerson(Person original)
    {
        return original with { FirstName = "Modified" };
    }
    
    // Raw string literals (C# 11+)
    public string RawStringLiteral = """
        This is a raw string literal.
        It can contain "quotes" without escaping.
        And multiple lines.
        """;
    
    // UTF-8 string literals (C# 11+)
    public ReadOnlySpan<byte> Utf8String => "Hello"u8;
    
    // Generic math (C# 11+)
    public T Add<T>(T left, T right) where T : INumber<T>
    {
        return left + right;
    }
    #endregion
    
    #region Nested Types
    public class NestedClass
    {
        public void NestedMethod() => Console.WriteLine("Nested");
    }
    
    public interface INestedInterface
    {
        void NestedInterfaceMethod();
    }
    
    public struct NestedStruct
    {
        public int Value { get; set; }
    }
    
    public enum NestedEnum
    {
        Option1,
        Option2
    }
    
    public delegate void NestedDelegate();
    #endregion
}
#endregion

#region Lambda Expressions and Anonymous Types
public class LambdaExamples
{
    public void DemonstrateLambdas()
    {
        // Expression lambda
        Func<int, int> square = x => x * x;
        
        // Statement lambda
        Action<string> print = message =>
        {
            Console.WriteLine($"Message: {message}");
            Console.WriteLine($"Length: {message.Length}");
        };
        
        // Lambda with multiple parameters
        Func<int, int, int> add = (x, y) => x + y;
        
        // Natural type lambda (C# 10+)
        var naturalLambda = (int x) => x * 2;
        
        // Lambda with attributes (C# 10+)
        var lambdaWithAttribute = ([NotNull] string s) => s.ToUpper();
        
        // Anonymous type
        var anonymous = new
        {
            Name = "Anonymous",
            Value = 42,
            Nested = new { Inner = true }
        };
    }
}
#endregion

#region Exception Handling
public class ExceptionExamples
{
    public void ExceptionHandling()
    {
        try
        {
            throw new InvalidOperationException("Test");
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("Test"))
        {
            Console.WriteLine("Caught with filter");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"General catch: {ex.Message}");
            throw; // Rethrow
        }
        finally
        {
            Console.WriteLine("Finally block");
        }
    }
    
    // Custom exception
    public class CustomException : Exception
    {
        public int ErrorCode { get; }
        
        public CustomException(string message, int errorCode) : base(message)
        {
            ErrorCode = errorCode;
        }
    }
}
#endregion

#region LINQ Examples
public class LinqExamples
{
    public void DemonstrateLinq()
    {
        var numbers = Enumerable.Range(1, 100);
        
        // Query syntax
        var query = from n in numbers
                    where n % 2 == 0
                    orderby n descending
                    select n * n;
        
        // Method syntax
        var method = numbers
            .Where(n => n % 2 == 0)
            .OrderByDescending(n => n)
            .Select(n => n * n);
        
        // Group by
        var grouped = from n in numbers
                      group n by n % 10 into g
                      select new { Remainder = g.Key, Numbers = g.ToList() };
        
        // Join
        var list1 = new[] { 1, 2, 3 };
        var list2 = new[] { 2, 3, 4 };
        var joined = from a in list1
                     join b in list2 on a equals b
                     select new { a, b };
    }
}
#endregion

#region Unsafe Code
public unsafe class UnsafeExamples
{
    public void UnsafeMethod()
    {
        int value = 42;
        int* pointer = &value;
        *pointer = 100;
        
        fixed (char* p = "Hello")
        {
            char* current = p;
            while (*current != '\0')
            {
                Console.Write(*current);
                current++;
            }
        }
    }
    
    // Stackalloc
    public void StackAllocExample()
    {
        Span<int> numbers = stackalloc int[10];
        for (int i = 0; i < numbers.Length; i++)
        {
            numbers[i] = i;
        }
    }
}
#endregion

#region Preprocessor Directives
public class PreprocessorExamples
{
    public void ConditionalCompilation()
    {
#if DEBUG
        Console.WriteLine("Debug mode");
#elif RELEASE
        Console.WriteLine("Release mode");
#else
        Console.WriteLine("Other mode");
#endif

#warning This is a warning
        
#pragma warning disable CS0168 // Variable declared but not used
        int unused;
#pragma warning restore CS0168
        
#region Collapsible Region
        Console.WriteLine("Inside region");
#endregion
    }
    
    [Conditional("DEBUG")]
    public void DebugOnlyMethod()
    {
        Console.WriteLine("This only runs in debug");
    }
}
#endregion

#region Entry Point
public class Program
{
    // Traditional entry point
    public static void Main(string[] args)
    {
        Console.WriteLine("Comprehensive C# Language Features Demo");
    }
    
    // Alternative async entry point
    public static async Task<int> MainAsync(string[] args)
    {
        await Task.Delay(100);
        return 0;
    }
}

// Top-level statements (C# 9+) would typically replace the Program class
// Console.WriteLine("Top-level program");
#endregion

// File-local type (C# 11+)
file class FileLocalClass
{
    public void FileLocalMethod() => Console.WriteLine("File-local");
}

#region Top-Level Statements and Declarations (C# 9+)
// Note: In a real file, these would typically be at the very top of the file
// and there can only be one file with top-level statements per project.
// They're included here for parser testing purposes.

// Top-level statements
Console.WriteLine("Top-level statement 1");
var topLevelVariable = 42;
Console.WriteLine($"Top-level variable: {topLevelVariable}");

// Top-level local function
void TopLevelFunction(string message)
{
    Console.WriteLine($"Top-level function: {message}");
}

// Top-level method with expression body
int TopLevelAdd(int a, int b) => a + b;

// Top-level async method
async Task<string> TopLevelAsyncMethod()
{
    await Task.Delay(100);
    return "Async result";
}

// Top-level lambda assignment
var topLevelLambda = (string s) => s.ToUpper();
Func<int, int> topLevelSquare = x => x * x;

// Top-level pattern matching
object topLevelObj = "test";
var topLevelResult = topLevelObj switch
{
    string s => $"String: {s}",
    int n => $"Number: {n}",
    _ => "Unknown"
};

// Top-level using statement
using var topLevelStream = new MemoryStream();

// Top-level try-catch
try
{
    TopLevelFunction("Testing");
}
catch (Exception ex)
{
    Console.WriteLine($"Top-level exception: {ex.Message}");
}

// Top-level return statement (implicit Main method return)
return 0;
