# using Parameters
# Wiktionary = @with_kw (title="", id=ID(""), parentid=ID(""), timestamp="", contributor=(user="anonym", id=ID("")), comment="",
#                        model="", format="", text="", sha1="", ns="", revision="")
##NamedTuple{(:title, :id, :ns, :revision, :revision), Tuple{String, String, String, String}}
##convert(Wiktionary, (;[ "ns" => "a", "revision" => "b" ]...))
cd(expanduser("~/dev/julia/"))
using Pkg
Pkg.activate("wiktionary")
using Distributed
## addprocs(1)
@info "using $(nprocs()) prcs $(nworkers()) workers"

@everywhere    using Pkg
@everywhere    Pkg.activate("wiktionary")
@everywhere    Pkg.resolve()
@everywhere    Pkg.instantiate()
@everywhere begin
    println("loading TableAlchemy")
    cd(expanduser("~/dev/julia/"))
    using TableAlchemy
    println("worker ready")
end


# starting a remote-process is ease, yeay!
# mtree = addprocs(["mytree@mytree-analytics.de:2222"];
#                  tunnel = true,
#                  dir = "/home/mytree/deploy",
#                  exename = "/home/mytree/julia-1.1.0/bin/julia")


using ProgressMeter
prog = ProgressUnknown("Wiktionary indexing:")
dprog = ProgressUnknown("data:")

cache_size = 100
min_mem = 3*10^9
min_mem_juliadb = 10^9
@everywhere sleep_time = .001
inbox=RemoteChannel(()->Channel(cache_size))
db_channel = RemoteChannel(()->Channel(cache_size*10))

@everywhere errorfile = joinpath(expanduser("~"),"ParserAlchemy.err")
open(errorfile,"w") do io
end
@everywhere begin
    println("loading FilingForest")
    cd(expanduser("~/dev/julia/"))
    using FilingForest
    using FilingForest.Parser
    using FilingForest.OrgParser
    using FilingForest.Tokens
    using FilingForest.WikiParser
    function nextchunk(inbox, db_channel)
        while isopen(inbox)
            try
                @info "compiling" maxlog=1
                wikichunks(inbox, db_channel; errorfile=errorfile)
                @info "compiling done" maxlog=1
            catch e
                @warn "cannot parse meanings in wiktionary" exception=e
            end
            sleep(sleep_time)
        end
    end
    println("worker ready")
end

@info "start loading"

wc=mc=0
read_task = @async begin
    try
    parse_bz2(expanduser("~/data/dewiktionary-latest-pages-articles.xml.bz2")) do val, counter
        ## ProgressMeter.next!(prog; showvalues=[(:word, val.title), (:mem, Sys.free_memory()/10^6) ])
        put!(inbox, val)
        sleep(sleep_time)
    end
    catch e
        @error "xml parsing" exception=e
    end
    close(inbox)
    while isready(inbox) || isready(db_channel)
        sleep(1)
    end
    close(db_channel)
end
## bind(inbox, read_task) ## close inbox when reading is done

#parse_task = @async
## nextchunk(inbox, db_channel)
for p in workers()## [1:end-1]
    remote_do(nextchunk, p, inbox, db_channel)
end

@db_name TokenString "TokenString"
@db_name AbstractToken "AbstractToken"
@db_name LineContent "LineContent"
@db_name Token "Token"
@db_name Template{Token,LineContent} "Template"
@db_internjoin Template{Token,LineContent} false
@db_internjoin LineContent false
@db_internjoin Token true
TableAlchemy.vector_index(x::Type{<:Line}) = :line
TableAlchemy.vector_index(x::Type{<:Paragraph}) = :par
TableAlchemy.vector_index(x::Type{<:AbstractToken}) = :token

db_name(Template{Token,LineContent})

show_wiki(x) = let w=x.word, m=haskey(x,:meaning) ? ": $(x.meaning)" : ""
    r = "$w$m"
    if lastindex(r)>60        
        r[1:nextind(r,60)]
    else
        r
    end
end

struct TypePartitionChannel{I}
    cache::Dict{Tuple{String,Type},TableAlchemy.VectorCache}
    incoming::I
    length::Int
end
TypePartitionChannel(i::I, l=100) where I = TypePartitionChannel{I}(
    Dict{Tuple{String,Type},TableAlchemy.VectorCache}(),i, l)
Base.isopen(x::TypePartitionChannel) = !isempty(x.cache) || isready(x.incoming)
Base.isready(x::TypePartitionChannel) = isopen(x.incoming) ? isready(x.incoming) : !isempty(x.cache)

function Base.take!(c::TypePartitionChannel)
    s = x.size
    while isopen(c)
        while isready(c)
            (target,x) = take!(c.incoming)
            ##@show x=take!(c)
            T=NamedStruct{Symbol(target),typeof(x)}
            ProgressMeter.next!(dprog; showvalues=[(:entry, show_wiki(x)), (:mem_GB, Sys.free_memory()/10^9) ])
            v=get!(() -> TableAlchemy.VectorCache{T}(undef, s),
                   c.cache,(target,T))
            if isfull(v) || (Sys.free_memory() < 1.5*min_mem) ## tested on sercver
                r=collect(v)
                ## create a new to release objects
                v=c.cache[(target,T)] = TableAlchemy.VectorCache{T}(undef, s)
                ## empty!(v) ## n_,v_ = 1, Vector{T}(undef, cache_size)
                vector_cache_size = sum([length(y) for y in values(c.cache)])
                @info "processing" vector_cache_size Sys.free_memory()/10^9
                return (target,r)
            end
            push!(v,T(x))
        end
        sleep(0.01)
    end
    collect(first(values(x.cache)))
end

## target,v = typed_data()

import Dates
datetimenow = Dates.now()

@db_autoindex NamedStruct{:meaning}
@db_autoindex NamedStruct{:word}
JT = joined_pkeys_tuple(Token)
@pkeys NamedStruct{:meaning} NamedTuple{tuple(:word),Tuple{JT}}
@pkeys NamedStruct{:word} NamedTuple{tuple(:word),Tuple{JT}}

dryrun = false
pkeys_tuple(NamedStruct{:word})
pkeys_tuple(NamedStruct{:word, NamedTuple{tuple(:word),Tuple{Token}}})

output = expanduser("~/database/wiktionary-$datetimenow")
mkpath(output)
results = TypeDB(output)


typevecs=TypePartitionChannel(db_channel,100)
while isready(typevecs) || isopen(typevecs)
    (into,dat) = take!(typevecs)
    global target,v = into,dat
    println()
    @info "indexing $(length(v)) $(eltype(v))"
    println()
    if Sys.free_memory() < min_mem_juliadb
        @info "memory pressure: saving data" (:mem_GB, Sys.free_memory()/10^9)
        TableAlchemy.save(results)
        @info "saved data" (:mem_GB, Sys.free_memory()/10^9)
    end
    TableReference(eltype(v))
    db_name(eltype(v))
    db_type(eltype(v))
    # try
    if !dryrun
        db, ks, jk = TableAlchemy.push_pkeys!(
            results,
            emptykeys(v), TypedIterator(v));
    end
    v = nothing
    Sys.GC.gc()
    
    ## results[db,first(ks)...]
    ## ks
    # catch e
    #     @error "cannot push into db" exception=e
    # end
end

TableAlchemy.save(results)
