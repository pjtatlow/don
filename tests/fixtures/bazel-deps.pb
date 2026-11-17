JF
//:all	filegroup*//aliases:chosen*//generated:out*@dep//:foreign/+
//:roundtrip	filegroup*@dep//:roundtrip2.
//aliases:chosenalias*//branches:selected,(
//branches:a.txt*//visibility:private,(
//branches:b.txt*//visibility:private)%
//branches:choose_aconfig_setting]Y
//branches:selected	filegroup*//branches:a.txt*//branches:b.txt*//branches:choose_a51
//common:shared	filegroup*//common:shared.txt/+
//common:shared.txt*//visibility:privatezv
//generated:generatorgenrule*//generated:input.txt*,@bazel_tools//tools/genrule:genrule-setup.sh2//generated:out1-
//generated:input.txt*//visibility:private,"(
//generated:out//generated:generator