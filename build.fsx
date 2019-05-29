#r "paket:
    nuget Fake.Core.Target
    nuget Fake.Core.ReleaseNotes
    nuget Fake.IO.FileSystem
    nuget Fake.DotNet.MSBuild
    nuget Fake.DotNet.Cli
    nuget Fake.DotNet.Testing.XUnit2
//"

#load "build.imports.fsx"
#load "build.tools.fsx"

open Fake.IO
open Fake.IO.Globbing.Operators
open Fake.Core

open Tools

let solutions = Proj.settings |> Config.keys "Build"
let packages = Proj.settings |> Config.keys "Pack"

let clean () = !! "**/bin/" ++ "**/obj/" |> Shell.deleteDirs
let build () = solutions |> Proj.buildMany
let restore () = solutions |> Proj.restoreMany
let test () = Proj.testAll ()
let release () = packages |> Proj.packMany
let publish apiKey = packages |> Seq.iter (Proj.publishNugetOrg apiKey)

Target.create "Refresh" (fun _ ->
    // Proj.regenerateStrongName "Hangfire.Storage.MySql.snk"
    Proj.updateCommonTargets "Common.targets"
)

Target.create "Clean" (fun _ -> clean ())

Target.create "Restore" (fun _ -> restore ())

Target.create "Build" (fun _ -> build ())

Target.create "Rebuild" ignore

Target.create "Test" (fun _ -> test ())

Target.create "Release" (fun _ -> release ())

Target.create "Release:Nuget" (fun _ ->
    Proj.settings |> Config.valueOrFail "nuget" "accessKey" |> publish
)

open Fake.Core.TargetOperators

"Refresh" ==> "Restore" ==> "Build" ==> "Rebuild" ==> "Test" ==> "Release" ==> "Release:Nuget"
"Clean" ?=> "Restore"
"Clean" ==> "Rebuild"
"Build" ?=> "Test"

Target.runOrDefault "Build"
