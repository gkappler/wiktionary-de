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

@everywhere begin
    println("loading TableAlchemy")
    cd(expanduser("~/dev/julia/"))
    using Pkg
    Pkg.activate("wiktionary")
    Pkg.instantiate()
    using TableAlchemy
    ##using Test
    using BenchmarkTools
    ##using Glob
    ##using JuliaDB
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

cache_size = 1000
@everywhere sleep_time = .01
inbox=RemoteChannel(()->Channel(cache_size))
db_channel = RemoteChannel(()->Channel(cache_size*10))



@everywhere begin
    println("loading FilingForest")
    cd(expanduser("~/dev/julia/"))
    using Pkg
    Pkg.activate("wiktionary")
    using FilingForest
    using FilingForest.Parser
    using FilingForest.OrgParser
    using FilingForest.Tokens
    using FilingForest.WikiParser
    function nextchunk(inbox, db_channel)
        while isopen(inbox)
            try
                @info "compiling" maxlog=1
                wikichunks(inbox, db_channel)
                @info "compiling done" maxlog=1
            catch e
                @warn "cannot parse meanings in wiktionary" e
            end
            sleep(sleep_time)
        end
    end
    println("worker ready")
end

@info "start loading"

wc=mc=0
read_task = @async parse_bz2() do val, counter
    # open("lastwiki.wiki","w") do io
    #     print(io, val.revision.text)
    # end
    global wc, mc
    ProgressMeter.next!(prog; showvalues=[(:word, val.title) ])
    put!(inbox, val)
    sleep(sleep_time)
end
#bind(inbox, read_task)

#parse_task = @async
## nextchunk(inbox, words, meanings)
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

output = expanduser("~/database/wiktionary")
mkpath(output)
results = TypeDB(output)



using ResumableFunctions
@resumable function collect_target_types(c, size=2)
    d = Dict{Tuple{String,Type},TableAlchemy.VectorCache}()
    while isopen(c)
        (target,x) = take!(c)
        ProgressMeter.next!(dprog; showvalues=[(:target, target), (:data,x.word) ])
        ##@show x=take!(c)
        let T=typeof(x) 
            v=get!(() -> TableAlchemy.VectorCache{T}(undef, size),
                   d,(target,T))
            if isfull(v)
                r=collect(v)
                empty!(v) ## n_,v_ = 1, Vector{T}(undef, cache_size)
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

typed_data=collect_target_types(db_channel,cache_size)

if false
    v = Union{String,Int}["1",2,4,"2",5,8,"g","a"]
    v_=(("test",x) for x in repeat(v,10000))
    x=collect_target_types(v_)
    x()
    f(vv)=for x=collect_target_types(vv)
        x
    end
    ##
    @time f(v_)
    using Profile
    using ProfileView
    ##
    Profile.clear(); @profile f(v_)
    ProfileView.view()
end
#####


(target,v) = typed_data()

for (target,v) in typed_data
    @db_name eltype(v) target
    @db_autoindex eltype(v)
    @pkeys eltype(v) (:word, :numid)
    TableAlchemy.push_pkeys!(
        results,
        TableAlchemy.joining_key_values(results, emptykeys(v), v));
end

save(results)

## manifest_row(t, typeof(RM[1]); word="Hallo", order="[1]")

# using LibGit2
# author = LibGit2.Signature("James Imnab", "james@bictxt.de")
# wiki_git = expanduser("~/tmp/wiktionary.git")
# mkpath(wiki_git)
# repo = LibGit2.init(wiki_git)
# @time parse_bz2() do val, counter
#     wait_onwarn = false
#     commit_threshold = 100
#     pathfile = split( val.title, ":")
#     # if length(pathfile)==1
#     #     pathfile = ["Wiktionary"]
#     # end
#     file=pathfile[end]
#     relpath = String[]
#     i=firstindex(file)
#     while length(relpath)<3 && i <= lastindex(file)
#         push!(relpath, lowercase(string(file[i])))
#     end
#     file = joinpath(wiki_git,pathfile[1:end-1]...,relpath...,"$file.wiki")
#     mkpath(dirname(file))
#     # @show val.revision.text
#     open(file,"w") do io
#         print(io, val.revision.text)
#     end
#     LibGit2.add!(repo, file)
#     if counter % commit_threshold == 0
#         @info "commit...."
#         LibGit2.commit(repo, "incremental wiktionary commit"; author = author)
#         counter = 0
#     end
#     if false
#     end
#     ## print(val.revision.text)
# end


