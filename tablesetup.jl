TableAlchemy._db_name(::Type{<:NamedString}) = "Indent"

using ParserAlchemy.Tokens
using WikitextParser

clearnames!(results)
## long compile times in case of many token types -- enable later
## db_name!(results, NamedString, :Token)
## db_intern!(results, NamedString)
db_name!(results, Token, :Token)
db_intern!(results, Token)
db_name!(results, LinePrefix{NamedString}, :Prefix)
db_intern!(results, LinePrefix{NamedString})
## db_intern!(results, (:type => :Line , :field => :prefix), Vector{NamedString})
## db_name!(results, Vector{NamedString}, :Prefix)
db_name!(results, Line{NamedString,Token}, :Line)
db_name!(results, Node{Line{Token,AbstractToken}}, :Node)
db_name!(results, WikiLink, :WikiLink)
db_name!(results, String, :String)
db_name!(results, Symbol, :Symbol)
db_name!(results, TokenPair{Symbol,Vector{LineContent}}, :TokenPair)
db_name!(results, Template{Token,LineContent}, :Template)
db_name!(results, Pair{String,Paragraph}, :TemplateArgument)
db_name!(results, Paragraph{Token,LineContent}, :Paragraph)
db_name!(results, Pair{String,Vector{Line{Token,LineContent}}},
         Symbol("Pair{String,Paragraph}"))
vector_index!(results, Vector{Token}, :token)
## vector_index!(results, Vector{TemplateArgument}, :arg) ## index with this -- seems to be occuring as UnionAll, qualified
vector_index!(results, Vector{LineContent}, :token)
vector_index!(results, Paragraph{NamedString,Token}, :line)
## db_name!(results, TableAlchemy.result_type(results,:meaning), nothing)
results.type_names
## TableAlchemy.result_type(results,:meaning)



## take!(typevecs; log=show_wiki) 
## take!(db_channel)
## take!(inbox)
import ParserAlchemy.Tokens: token_lines
token_lines(x::NamedTuple{ns,ts}) where {ns,ts} =
    (; ( ( n => token_lines(getproperty(x,n); typenames=results.type_names))
         for (n,t) in zip(ns, fieldtypes(ts)) )...
     )
token_lines(x::NamedStruct{name}) where {name} =
    NamedStruct{name}(token_lines(get(x)))

token_lines(x) = x

rich_lines(x::NamedTuple{ns,ts};typenames=Main.results.type_names, kw...)  where {ns,ts} =
    (; ( ( n => rich_lines(getproperty(x,n); typenames=typenames, kw...))
         for (n,t) in zip(ns, fieldtypes(ts)) )...
     )
rich_lines(x::NamedStruct{name}; typenames=Main.results.type_names, kw...) where {name} =
    NamedStruct{name}(rich_lines(get(x); typenames=typenames, kw...))

rich_lines(x; kw...) = x

rich_lines(x::Paragraph{NamedString,Token}; typenames=Main.results.type_names, kw...) =
    unnest_lines(nested_wrap_types(nested(x); typenames=typenames, kw...))

rich_lines(x::TableAlchemy.IndexedVector{name}; typenames=Main.results.type_names, kw...) where name =
    rich_lines(x.data; typenames=typenames, kw...)



M=NamedStruct{:meaning}

W=NamedStruct{::word}

function Base.show(io::IO, x::Vector{<:Union{M,W}})
    for p in x
        println(io,p)
    end
end

function Base.show(io::IO, x::M)
    global results
    println(io, x.word, " ", x.order, " (", x.numid, ")")
    for p in propertynames(x)
        if !in(p, [ :word, :order, :numid ])
            v = getproperty(x,p)
            if !(v isa AbstractVector{<:Line} && isempty(v))
                println(io,"== ",p," ==")
                println(io,rich_lines(v; typenames=results.type_names))
            end
        end
    end
end

function Base.show(io::IO, x::W)
    global results
    println(io, x.word, " ", " (", x.numid, ")")
    for p in propertynames(x)
        if !in(p, [ :word, :numid ])
            v = getproperty(x,p)
            if !(v isa AbstractVector{<:Line} && isempty(v))
                println(io,"== ",p," ==")
                println(io,rich_lines(v; typenames=results.type_names))
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

