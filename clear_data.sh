base_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"/chrome_profiles

rm -rf "$base_dir"/*/DeferredBrowserMetrics/*
rm -rf "$base_dir"/*/component_crx_cache/*
rm -rf "$base_dir"/*/BrowserMetric*
rm -rf "$base_dir"/*/optimization_guide_model_store/*
rm -rf "$base_dir"/*/Default/optimization_guide_model_store/*
rm -rf "$base_dir"/*/Default/Cache/*