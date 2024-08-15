#!/bin/bash

fink=""
jar=""
apps=(

)

running_apps=`$fink list 2>/dev/null | awk '/RUNNING/ {print \$(NF -1)}'`

for app in ${apps[*]} ; do
  app_name=`echo $app | -F. '{print \$NF}'`

  if [[ "${running_apps[@]}" =~ "$app_name" ]]; then
    echo "$app_name 已启动，无需再次启动......"
  else
    echo "启动应用：$app_name"
    $fink run -d -c $app $jar
  fi
done