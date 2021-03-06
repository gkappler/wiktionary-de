TableAlchemy._db_name(::Type{<:NamedString}) = "Indent"

using ParserAlchemy.Tokens
using WikitextParser

## take!(typevecs; log=show_wiki) 
## take!(db_channel)
## take!(inbox)
import ParserAlchemy.Tokens: token_lines
token_lines(x::NamedTuple{ns,ts}; typenames) where {ns,ts} =
    (; ( ( n => token_lines(getproperty(x,n); typenames=typenames))
         for (n,t) in zip(ns, fieldtypes(ts)) )...
     )
token_lines(x::NamedStruct{name}; typenames) where {name} =
    NamedStruct{name}(token_lines(get(x); typenames=typenames))

token_lines(x; kw...) = x
## token_lines(x::Union{Token, String}; kw...) = x

rich_lines(x::NamedTuple{ns,ts};typenames, kw...)  where {ns,ts} =
    (; ( ( n => rich_lines(getproperty(x,n); typenames=typenames, kw...))
         for (n,t) in zip(ns, fieldtypes(ts)) )...
     )

rich_lines(x::NamedStruct{name}; typenames, kw...) where {name} =
    NamedStruct{name}(rich_lines(get(x); typenames=typenames, kw...))

rich_lines(x; kw...) = x

rich_lines(x::Paragraph{NamedString,Token}; typenames, kw...) =
    unnest_lines(nested_wrap_types(nested(x); typenames=typenames, kw...))

rich_lines(x::TableAlchemy.IndexedVector{name}; typenames, kw...) where name =
    rich_lines(x.data; typenames=typenames, kw...)



M=NamedStruct{:meaning}

W=NamedStruct{:word}

function Base.show(io::IO, x::Vector{<:Union{M,W}})
    for p in x
        println(io,p)
    end
end

function Base.show(io::IO, x::M)
    global results
    print(io, x.word, " ", x.order)
    hasfield(typeof(x),:numid) ? println(io, " (", x.numid, ")") : println(io)
    for p in propertynames(x)
        if !in(p, [ :word, :order, :numid ])
            v = getproperty(x,p)
            if !(v isa AbstractVector{<:Line} && isempty(v))
                println(io,"== ",p," ==")
                try
                    println(io,rich_lines(v; typenames=results.type_names))
                catch e
                    @error "rich_lines error" exception=e
                end
            end
        end
    end
end

function Base.show(io::IO, x::W)
    global results
    println(io, x.word)
    hasfield(typeof(x),:numid) ? println(io, " (", x.numid, ")") : println(io)
    for p in propertynames(x)
        if !in(p, [ :word, :numid ])
            v = getproperty(x,p)
            if !(v isa AbstractVector{<:Line} && isempty(v))
                println(io,"== ",p," ==")
                try
                    println(io,rich_lines(v; typenames=results.type_names))
                catch e
                    @error "rich_lines error" exception=e
                end
            end
        end
    end
end


using AbstractTrees
import AbstractTrees: printnode


function AbstractTrees.printnode(io::IO, x::M)
    global results
    print(io, x.word, " ", x.order, ": ", rich_lines(x.meaning; typenames=results.type_names))
end

function AbstractTrees.printnode(io::IO, e::Pair{<:Any,Token})
    global results
    x=e.second
    print(io, x)
end

function AbstractTrees.printnode(io::IO, e::Pair{<:Any,M})
    global results
    x=e.second
    print(io, x.word, " ", x.order, ": ", rich_lines(x.meaning; typenames=results.type_names))
end


function WikiDB!(results)
    clearnames!(results)
    db_name!(results, Token, :Token)
    db_intern!(results, Token)
    db_name!(results, LinePrefix{NamedString}, :Prefix)
    db_intern!(results, LinePrefix{NamedString})
    db_name!(results, Line{NamedString,AbstractToken}, :Line)
    db_name!(results, Line{NamedString,Token}, :Line)
    db_name!(results, Node{Token,Line{NamedString,AbstractToken}}, :Node)
    db_name!(results, Node{Token,Line{NamedString,AbstractToken}}, :Node) ## Tables
    db_name!(results, WikiLink, :WikiLink)
    db_name!(results, String, :String)
    db_name!(results, Symbol, :Symbol)
    db_name!(results, TemplateParameter, :TemplateParameter)
    db_name!(results, TokenPair{Symbol,Vector{LineContent}}, :TokenPair)
    db_name!(results, TokenPair{Symbol,Vector{Token}}, :TokenPairToken)
    db_name!(results, Template{NamedString,LineContent}, :Template)
    db_name!(results, Pair{String,Token}, Symbol("Pair{String,Token}"))
    db_name!(results, Pair{String,Token}, :TypedToken)
    db_name!(results, Pair{String,Vector{Line{NamedString,LineContent}}}, Symbol("Pair{String,Paragraph}")) ## fallback
    db_name!(results, Pair{String,Vector{Line{NamedString,LineContent}}}, :TemplateArgument)
    db_name!(results, Paragraph{NamedString,LineContent}, :Paragraph)
    vector_index!(results, Vector{Token}, :token)
    vector_index!(results, Vector{LineContent}, :token)
    vector_index!(results, Paragraph{NamedString,Token}, :line)
    results.type_names
end

