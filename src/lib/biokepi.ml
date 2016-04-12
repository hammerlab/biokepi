
module Pipeline = Biokepi_pipeline_edsl.Pipeline

module KEDSL = Biokepi_run_environment.Common.KEDSL

module Metadata = Biokepi_run_environment.Metadata

module Machine = Biokepi_run_environment.Machine

module Tools = Biokepi_bfx_tools

module Setup = struct

  include Biokepi_environment_setup

end

