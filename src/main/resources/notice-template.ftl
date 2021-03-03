<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->

<#function projectLicense licenses>
    <#if dependencyMap?size == 0>
        <#list licenses as license>
            <#return license>
        </#list>
    <#else>
        <#assign result = ""/>
        <#list licenses as license>
            <#if result == "">
                <#assign result = license/>
            <#else>
                <#assign result = result + ", " + license/>
            </#if>
        </#list>
        <#return result>
    </#if>
</#function>

<#function projectName p>
    <#if p.name?index_of('Unnamed') &gt; -1>
        <#return p.artifactId>
    <#else>
        <#return p.name>
    </#if>
</#function>

<#if dependencyMap?size == 0>
    The project has no dependencies.
<#else>

================================================================================

zentity uses the following third-party dependencies:

    <#list dependencyMap as e>
        <#assign project = e.getKey()/>
        <#assign licenses = e.getValue()/>
--------------------------------------------------------------------------------

Name:     ${projectName(project)}
Artifact: ${project.groupId}:${project.artifactId}:${project.version}
URL:      ${project.url!"-"}
License:  ${projectLicense(licenses)}

    </#list>
</#if>
--------------------------------------------------------------------------------
