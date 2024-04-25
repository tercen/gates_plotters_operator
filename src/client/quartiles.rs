pub fn quartiles(data: Vec<f64>) -> (f64, f64, f64) {
    if data.len() == 0 {
        return (f64::NAN, f64::NAN, f64::NAN);
    } else if data.len() == 1 {
        return (data[0], data[0], data[0]);
    } else if data.len() == 2 {
        return (data[0], (data[0] + data[1]) / 2.0, data[1]);
    } else {
        let q1;
        let q2;
        let q3;
        let q2_i = data.len() / 2;
        if data.len() % 2 == 0 {
            q2 = (data[q2_i - 1] + data[q2_i]) / 2.0;
        } else {
            q2 = data[q2_i];
        }

        let q1_i = q2_i / 2;
        let q3_i = q2_i + (q2_i - q1_i);
        if q1_i % 2 == 0 {
            if q1_i < 1 {
                q1 = data[q1_i];
            } else {
                q1 = (data[q1_i - 1] + data[q1_i]) / 2.0;
            }

            q3 = (data[q3_i - 1] + data[q3_i]) / 2.0;
        } else {
            q1 = data[q1_i];
            q3 = data[q3_i];
        }
        (q1, q2, q3)
    }
}
