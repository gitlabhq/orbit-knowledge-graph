
class ApplicationController < ActionController::Base
  def set_user
    @user = User.find(params[:id])
  end
end

class UsersController < ApplicationController
  before_action :set_user, only: [:show, :edit]

  def show
    # @user is set by before_action calling set_user
    render json: @user # Ref 1: @user usage - tracing assignment might be complex
  end

  def create
    @user = User.new(user_params) # Assignment
    if @user.save # Ref 2: @user usage - should trace to User.new
       # ...
    end
  end

  private

def user_params
    params.require(:user).permit(:name, :email) # Ref 3: Direct call
  end
end

# Simulate model
module User
  def self.find(id); end
  def self.new(params); end
  def save; end
end

module ActionController; class Base; end; end
module ActionDispatch; end
