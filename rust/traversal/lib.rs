/*
 * Copyright (C) 2023 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

extern crate cxx;
extern crate ortools;

use cxx::let_cxx_string;
use std::pin::Pin;

pub fn optimise() {
    println!("=== ORTOOLS ===");
    println!("Going to solve this problem:");
    println!();
    println!("Maximize x + 10y subject to the following constraints:
        x + 7 y	≤	17.5
        x	≤	3.5
        x	≥	0
        y	≥	0
        x, y integers");
    println!();

    let mut solver = ortools::ffi::new_mpsolver_scip();

    let_cxx_string!(x = "x");
    let_cxx_string!(y = "y");

    let var_x = solver.pin_mut().MakeIntVar(0.0, f64::INFINITY, &x);
    let var_y = solver.pin_mut().MakeIntVar(0.0, f64::INFINITY, &y);

    println!("Number of variables = {}", solver.NumVariables());

    let_cxx_string!(c0 = "c0");
    let_cxx_string!(c1 = "c1");
    let con0 = solver.pin_mut().MakeRowConstraint(-f64::INFINITY, 17.5, &c0);

    unsafe {
        Pin::new_unchecked(con0.as_mut().unwrap()).SetCoefficient(var_x, 1.0);
        Pin::new_unchecked(con0.as_mut().unwrap()).SetCoefficient(var_y, 7.0);
    }

    let con1 = solver.pin_mut().MakeRowConstraint(-f64::INFINITY, 3.5, &c1);

    unsafe {
        Pin::new_unchecked(con1.as_mut().unwrap()).SetCoefficient(var_x, 1.0);
        Pin::new_unchecked(con1.as_mut().unwrap()).SetCoefficient(var_y, 0.0);
    }

    let objective = solver.pin_mut().MutableObjective();
    unsafe {
        Pin::new_unchecked(objective.as_mut().unwrap()).SetCoefficient(var_x, 1.0);
        Pin::new_unchecked(objective.as_mut().unwrap()).SetCoefficient(var_y, 10.0);
        Pin::new_unchecked(objective.as_mut().unwrap()).SetMaximization();

    }

    let result = solver.pin_mut().Solve();
    unsafe {
        println!("Solution: result: {:?}", result);
        println!("Objective value: {}", objective.as_ref().unwrap().Value());
        println!("X = {}", var_x.as_ref().unwrap().solution_value());
        println!("Y = {}", var_y.as_ref().unwrap().solution_value());
        println!()
    }
}