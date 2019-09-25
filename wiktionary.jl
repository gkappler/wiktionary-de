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

using ResumableFunctions
@resumable function collect_target_types(c, size=2)
    d = Dict{Tuple{String,Type},TableAlchemy.VectorCache}()
    while isready(c) || isopen(c)
        (target,x) = take!(c)
        ProgressMeter.next!(dprog; showvalues=[(:entry, show_wiki(x)), (:mem_GB, Sys.free_memory()/10^9) ])
        ##@show x=take!(c)
        let T=typeof(x) 
            v=get!(() -> TableAlchemy.VectorCache{T}(undef, size),
                   d,(target,T))
            if isfull(v) || Sys.free_memory() < 1.5*min_mem ## tested on sercver
                r=collect(v)
                ## create a new to release objects
                d[(target,T)] = TableAlchemy.VectorCache{T}(undef, size)
                @yield (target,r)
            end
            push!(v,x)
        end
    end
    for (k,v) = pairs(d)
        if length(v)>1
            @yield (k[1],collect(v))
        end
    end
end

import Dates
datetimenow = Dates.now()
output = expanduser("~/database/wiktionary-$datetimenow")
mkpath(output)
results = TypeDB(output)

typed_data=collect_target_types(db_channel,100)

for (target,v) in typed_data
    if Sys.free_memory() < min_mem
        @info "memory pressure: saving data" (:mem_GB, Sys.free_memory()/10^9)
        TableAlchemy.save(results)
        Sys.GC.gc()
        @info "saved data" (:mem_GB, Sys.free_memory()/10^9)
    end
    @db_name eltype(v) target
    @db_autoindex eltype(v)
    @pkeys eltype(v) (:word,)
    TableReference(eltype(v))
    db_type(eltype(v))
    # try
        db, ks, jk = TableAlchemy.push_pkeys!(
            results,
            emptykeys(v), TypedIterator(v));
    
    ## results[db,first(ks)...]
    ## ks
    # catch e
    #     @error "cannot push into db" exception=e
    # end
end

TableAlchemy.save(results)
