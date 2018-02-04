#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

data_plane_api_commit="${1:-HEAD}"
gogoproto_commit="${2:-HEAD}"
googleapis_commit="${3:-HEAD}"
lyft_protoc_gen_validate_commit="${4:-HEAD}"
opencensus_commit="${5:-HEAD}"
prometheus_metrics_model_commit="${6:-HEAD}"

protodir="${__dir}/../api/src/main/proto"
tmpdir=`mktemp -d 2>/dev/null || mktemp -d -t 'tmpdir'`

# Check if the temp dir was created.
if [[ ! "${tmpdir}" || ! -d "${tmpdir}" ]]; then
  echo "Could not create temp dir"
  exit 1
fi

# Clean up the temp directory that we created.
function cleanup {
  rm -rf "${tmpdir}"
}

# Register the cleanup function to be called on the EXIT signal.
trap cleanup EXIT

pushd "${tmpdir}" >/dev/null

rm -rf "${protodir}"

curl -sL https://github.com/envoyproxy/data-plane-api/archive/${data_plane_api_commit}.tar.gz | tar xz --include="*.proto"
# TODO: Create VERSION file with the data plane api commit
mkdir -p "${protodir}/envoy"
cp -r data-plane-api-*/envoy/* "${protodir}/envoy"

curl -sL https://github.com/gogo/protobuf/archive/${gogoproto_commit}.tar.gz | tar xz --include="*.proto"
# TODO: Create VERSION file with the gogoproto commit
mkdir -p "${protodir}/gogoproto"
cp protobuf-*/gogoproto/gogo.proto "${protodir}/gogoproto"

curl -sL https://github.com/googleapis/googleapis/archive/${googleapis_commit}.tar.gz | tar xz --include="*.proto"
# TODO: Create VERSION file with the googleapis commit
mkdir -p "${protodir}/google/api"
mkdir -p "${protodir}/google/rpc"
cp googleapis-*/google/api/annotations.proto googleapis-*/google/api/http.proto "${protodir}/google/api"
cp googleapis-*/google/rpc/status.proto "${protodir}/google/rpc"

curl -sL https://github.com/lyft/protoc-gen-validate/archive/${lyft_protoc_gen_validate_commit}.tar.gz | tar xz --include="*.proto"
# TODO: Create VERSION file with the lyft protoc-gen-validate commit
mkdir -p "${protodir}/validate"
cp -r protoc-gen-validate-*/validate/* "${protodir}/validate"

curl -sL https://github.com/census-instrumentation/opencensus-proto/archive/${opencensus_commit}.tar.gz | tar xz --include="*.proto"
# TODO: Create VERSION file with the opencensus commit
cp opencensus-proto-*/opencensus/proto/trace/trace.proto "${protodir}"

curl -sL https://github.com/prometheus/client_model/archive/${prometheus_metrics_model_commit}.tar.gz | tar xz --include="*.proto"
# TODO: Create VERSION file with the prometheus metrics model commit
cp client_model-*/metrics.proto "${protodir}"

popd >/dev/null
