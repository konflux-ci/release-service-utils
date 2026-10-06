# Query from filter-already-released-advisory-rpms-task
# release-service-catalog commit 9d01d3ac4a8770a9b32f799ce6ef62765881f291
  . as $entries | $map[0] as $m |
  reduce $entries[] as $e (
    {unreleased: [], in_advisory: [], advisories: {}};
    if $m[$e.purl] then
      .in_advisory += [$e]
      | .advisories[$m[$e.purl]] = true
    else
      .unreleased += [$e]
    end
  ) | {
    unreleased,
    in_advisory,
    advisories: (.advisories | keys)
  }
