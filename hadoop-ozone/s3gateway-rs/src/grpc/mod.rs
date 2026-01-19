pub mod om;

pub mod common {
    tonic::include_proto!("hadoop.common");
}

pub mod hdds {
    tonic::include_proto!("hadoop.hdds");
}

pub mod proto {
    tonic::include_proto!("hadoop.ozone");
}
